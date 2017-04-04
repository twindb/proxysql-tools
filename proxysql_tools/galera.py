from proxysql_tools import log
from proxysql_tools.entities.galera import (
    LOCAL_STATE_SYNCED,
    LOCAL_STATE_DONOR_DESYNCED
)
from proxysql_tools.entities.proxysql import BACKEND_STATUS_ONLINE
from proxysql_tools.managers.galera_manager import (
    GaleraManager,
    GaleraNodeNonPrimary,
    GaleraNodeUnknownState
)
from proxysql_tools.managers.proxysql_manager import (
    ProxySQLManager,
    ProxySQLAdminConnectionError
)


def register_cluster_with_proxysql(proxy_cfg, galera_cfg):
    """Register a Galera cluster within ProxySQL. The nodes in the cluster
    will be distributed between writer hostgroup and reader hostgroup.

    :param config: The config object.
    :type config: ConfigParser.ConfigParser
    :param proxy_cfg: The ProxySQL config object.
    :type proxy_cfg: ProxySQLConfig
    :param galera_cfg: The Galera config object.
    :type galera_cfg: GaleraConfig
    :return: Returns True on success, False otherwise.
    :rtype: bool
    """
    hostgroup_writer = galera_cfg.writer_hostgroup_id
    hostgroup_reader = galera_cfg.reader_hostgroup_id

    # We also check that the initial node that is being used to register the
    # cluster with ProxySQL is actually a healthy node and part of the primary
    # component.
    galera_man = GaleraManager(galera_cfg.cluster_host,
                               galera_cfg.cluster_port,
                               galera_cfg.cluster_username,
                               galera_cfg.cluster_password)
    try:
        galera_man.discover_cluster_nodes()
    except GaleraNodeNonPrimary:
        log.error('Cluster node %s:%s used for registration is non-primary.' %
                  (galera_man.host, galera_man.port))
        return False
    except GaleraNodeUnknownState:
        log.error('Cluster node %s:%s used for registration is in unknown '
                  'state.' % (galera_man.host, galera_man.port))
        return False

    # First we try to find nodes in synced state.
    galera_nodes_synced = [n for n in galera_man.nodes
                           if n.local_state == LOCAL_STATE_SYNCED]
    galera_nodes_desynced = [n for n in galera_man.nodes
                             if n.local_state == LOCAL_STATE_DONOR_DESYNCED]

    # If we found no nodes in synced or donor/desynced state then we
    # cannot continue.
    if not galera_nodes_synced and not galera_nodes_desynced:
        log.error('No node found in SYNCED or DESYNCED state.')
        return False

    proxysql_man = ProxySQLManager(proxy_cfg.host,
                                   proxy_cfg.admin_port,
                                   proxy_cfg.admin_username,
                                   proxy_cfg.admin_password,
                                   reload_runtime=False)

    try:
        # We also validate that we can connect to ProxySQL
        proxysql_man.ping()

        # Setup the monitoring user used by ProxySQL to monitor the backends
        setup_proxysql_monitoring_user(proxysql_man,
                                       proxy_cfg.monitor_username,
                                       proxy_cfg.monitor_password)

        # Let's remove all the nodes defined in the hostgroups that are not
        # part of this cluster or are not in desired state.
        if galera_nodes_synced:
            desired_state = LOCAL_STATE_SYNCED
            nodes_list = galera_nodes_synced
        else:
            desired_state = LOCAL_STATE_DONOR_DESYNCED
            nodes_list = galera_nodes_desynced

        # Handle write backends
        writer_backends = deregister_unhealthy_backends(proxysql_man,
                                                        galera_man.nodes,
                                                        hostgroup_writer,
                                                        [desired_state])

        # If there are more than one nodes in the writer hostgroup then we
        # remove all but one.
        if len(writer_backends) > 1:
            log.info('There are %d writers. Removing all but one.',
                     len(writer_backends))
            for backend in writer_backends[1:]:
                proxysql_man.deregister_backend(hostgroup_writer,
                                                backend.hostname,
                                                backend.port)
        elif len(writer_backends) == 0:
            # If there are no backends registered in the writer hostgroup
            # then we register one healthy galera node.
            node = nodes_list[0]
            proxysql_man.register_backend(hostgroup_writer,
                                          node.host, node.port)

        # Fetch the final list of writer backends
        writer_backend = [b for b in
                          proxysql_man.fetch_backends(hostgroup_writer)
                          if b.status == BACKEND_STATUS_ONLINE][0]

        # Now deregister all the unhealthy backends in reader hostgroup
        reader_backends = deregister_unhealthy_backends(proxysql_man,
                                                        galera_man.nodes,
                                                        hostgroup_reader,
                                                        [desired_state])

        # Now we register all of the healthy galera nodes in the
        # reader hostgroup.
        for node in nodes_list:
            if (len(reader_backends) > 0 and
                    node.host == writer_backend.hostname and
                    node.port == writer_backend.port):
                continue

            proxysql_man.register_backend(hostgroup_reader,
                                          node.host, node.port)

        # Now filter healthy backends that are common between writer hostgroup
        # and reader hostgroup
        reader_backends = [b for b in
                           proxysql_man.fetch_backends(hostgroup_reader)
                           if b.status == BACKEND_STATUS_ONLINE]

        # If we have more than one backend registered in the reader hostgroup
        # then we remove the ones that are also present in the writer hostgroup
        if len(reader_backends) > 1:
            for b in reader_backends:
                if (b.hostname == writer_backend.hostname and
                        b.port == writer_backend.port):
                    log.info("There is more than one backend registered "
                             "in the reader hostgroup. We remove the ones "
                             "that are also present in the writer hostgroup")
                    proxysql_man.deregister_backend(hostgroup_reader,
                                                    b.hostname, b.port)

        # TODO: Add user sync functionality that syncs MySQL users with Proxy.
    except ProxySQLAdminConnectionError:
        log.error('ProxySQL connection failed.')
        return False
    finally:
        # Reload the ProxySQL runtime so that it picks up all the changes
        # that have been made so far.
        proxysql_man.reload_runtime()

    return True


def deregister_unhealthy_backends(proxysql_man, galera_nodes, hostgroup_id,
                                  desired_states):
    """Remove backends in a particular hostgroup that are not in the Galera
    cluster or whose state is not in one of the desired states.

    :param proxysql_man: ProxySQL manager corresponding to the ProxySQL
        instance.
    :type proxysql_man: ProxySQLManager
    :param galera_nodes: List of Galera nodes.
    :type galera_nodes: list[GaleraNode]
    :param hostgroup_id: The ID of the ProxySQL hostgroup.
    :type hostgroup_id: int
    :param desired_states: Nodes not in this list of states are considered
        unhealthy.
    :type desired_states: list[str]
    :return: A list of ProxySQL backends that correspond to the Galera nodes
        that are part of the cluster.
    :rtype: list[ProxySQLMySQLBackend]
    """
    backend_list = proxysql_man.fetch_backends(hostgroup_id)
    for backend in backend_list:
        # Find the matching galera node and then see if the node state is
        # synced or donor/desynced. If not one of those two states then we
        # deregister the node from ProxySQL as well.
        backend_found_in_cluster = False
        if backend.status == BACKEND_STATUS_ONLINE:
            for node in galera_nodes:
                if (node.host == backend.hostname and node.port == backend.port
                        and node.local_state in desired_states):
                    backend_found_in_cluster = True
                    break

        if not backend_found_in_cluster:
            log.warning('Backend not found in the cluster: '
                        'hostgroup %d, %s:%s %s',
                        hostgroup_id,
                        backend.hostname, backend.port,
                        backend.status)
            proxysql_man.deregister_backend(hostgroup_id, backend.hostname,
                                            backend.port)

            # Remove the backend from list of writer backends as well.
            backend_list.remove(backend)

    return backend_list


def setup_proxysql_monitoring_user(proxysql_man, monitor_user, monitor_pass):
    """Setup the monitoring user used by ProxySQL to monitor the backends.

    :param proxysql_man: ProxySQL manager corresponding to the
        ProxySQL instance.
    :type proxysql_man: ProxySQLManager
    :param monitor_user: The user used by ProxySQL to monitor the backends.
    :type monitor_user: str
    :param monitor_pass: The password of user used by ProxySQL to monitor
        the backends.
    :type monitor_pass: str
    """
    proxysql_man.set_var('mysql-monitor_username', monitor_user)
    proxysql_man.set_var('mysql-monitor_password', monitor_pass)


def register_mysql_users_with_proxysql():
    pass
