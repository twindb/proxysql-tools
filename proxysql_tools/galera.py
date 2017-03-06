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


def register_cluster_with_proxysql(proxy_host, proxy_admin_port,
                                   proxy_admin_user, proxy_admin_pass,
                                   hostgroup_writer, hostgroup_reader,
                                   cluster_host, cluster_port, cluster_user,
                                   cluster_pass, proxy_monitor_user,
                                   proxy_monitor_pass):
    """Register a Galera cluster within ProxySQL. The nodes in the cluster
    will be distributed between writer hostgroup and reader hostgroup.

    :param str proxy_host: The ProxySQL host.
    :param int proxy_admin_port: The ProxySQL admin port.
    :param str proxy_admin_user: The ProxySQL admin user.
    :param str proxy_admin_pass: The ProxySQL admin password.
    :param int hostgroup_writer: The ID of writer ProxySQL hostgroup.
    :param int hostgroup_reader: The ID of reader ProxySQL hostgroup.
    :param str cluster_host: Hostname of a node in Galera cluster.
    :param int cluster_port: Port of a node in Galera cluster.
    :param str cluster_user: MySQL username of a user in Galera cluster.
    :param str cluster_pass: MySQL password of a user in Galera cluster.
    :param str proxy_monitor_user: The user used by ProxySQL to monitor
        the backends.
    :param str proxy_monitor_pass: The password of user used by ProxySQL to
        monitor the backends.
    :return bool: Returns True on success, False otherwise.
    """
    # TODO: Add user sync functionality that syncs MySQL users with ProxySQL.

    # We also check that the initial node that is being used to register the
    # cluster with ProxySQL is actually a healthy node and part of the primary
    # component.
    galera_man = GaleraManager(cluster_host, cluster_port,
                               cluster_user, cluster_pass)
    try:
        galera_man.discover_cluster_nodes()
    except GaleraNodeNonPrimary:
        log.error('Cluster node %s:%s used for registration is non-primary.' %
                  (cluster_host, cluster_port))
        return False
    except GaleraNodeUnknownState:
        log.error('Cluster node %s:%s used for registration is in unknown '
                  'state.' % (cluster_host, cluster_port))
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

    proxysql_man = ProxySQLManager(proxy_host, proxy_admin_port,
                                   proxy_admin_user, proxy_admin_pass,
                                   reload_runtime=False)

    try:
        # We also validate that we can connect to ProxySQL
        proxysql_man.ping()

        # Setup the monitoring user used by ProxySQL to monitor the backends
        setup_proxysql_monitoring_user(proxysql_man, proxy_monitor_user,
                                       proxy_monitor_pass)

        for hostgroup_id in [hostgroup_writer, hostgroup_reader]:
            # Let's remove all the nodes defined in the hostgroups that are not
            # part of this cluster or are not in desired state.
            if galera_nodes_synced:
                desired_state = LOCAL_STATE_SYNCED
                nodes_list = galera_nodes_synced
            else:
                desired_state = LOCAL_STATE_DONOR_DESYNCED
                nodes_list = galera_nodes_desynced

            backends_list = deregister_unhealthy_backends(proxysql_man,
                                                          galera_man.nodes,
                                                          hostgroup_id,
                                                          [desired_state])

            # If there are more than one nodes in the writer hostgroup then we
            # remove all but one.
            if len(backends_list) > 1 and hostgroup_id == hostgroup_writer:
                for backend in backends_list[1:]:
                    proxysql_man.deregister_backend(hostgroup_writer,
                                                    backend.hostname,
                                                    backend.port)

            if len(backends_list) == 0:
                # If there are no backends registered in the writer hostgroup
                # then we register one healthy galera node.
                if hostgroup_id == hostgroup_writer:
                    node = nodes_list[0]
                    proxysql_man.register_backend(hostgroup_writer,
                                                  node.host, node.port)

                # If there are no backends registered in the reader hostgroup
                # then we register all of the healthy galera nodes.
                if hostgroup_id == hostgroup_reader:
                    for node in nodes_list:
                        proxysql_man.register_backend(hostgroup_reader,
                                                      node.host, node.port)

        # Now filter healthy backends that are common between writer hostgroup
        # and reader hostgroup
        writer_backend = [b for b in
                          proxysql_man.fetch_backends(hostgroup_writer)
                          if b.status == BACKEND_STATUS_ONLINE][0]
        reader_backends = [b for b in
                           proxysql_man.fetch_backends(hostgroup_reader)
                           if b.status == BACKEND_STATUS_ONLINE]

        # If we have more than one backend registered in the reader hostgroup
        # then we remove the ones that are also present in the writer hostgroup
        if len(reader_backends) > 1:
            for b in reader_backends:
                if (b.hostname == writer_backend.hostname and
                        b.port == writer_backend.port):
                    proxysql_man.deregister_backend(hostgroup_reader,
                                                    b.hostname, b.port)
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

    :param ProxySQLManager proxysql_man: ProxySQL manager corresponding to the
        ProxySQL instance.
    :param list[GaleraNode] galera_nodes: List of Galera nodes.
    :param int hostgroup_id: The ID of the ProxySQL hostgroup.
    :param list[str] desired_states: Nodes not in this list of states are
        considered unhealthy.
    :return list[GaleraNode]: List of backends that correspond to the Galera
        nodes that are part of the cluster.
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
            proxysql_man.deregister_backend(hostgroup_id, backend.hostname,
                                            backend.port)

            # Remove the backend from list of writer backends as well.
            backend_list.remove(backend)

    return backend_list


def setup_proxysql_monitoring_user(proxysql_man, monitor_user, monitor_pass):
    """Setup the monitoring user used by ProxySQL to monitor the backends.

    :param ProxySQLManager proxysql_man: ProxySQL manager corresponding to the
        ProxySQL instance.
    :param str monitor_user: The user used by ProxySQL to monitor the backends.
    :param str monitor_pass: The password of user used by ProxySQL to monitor
        the backends.
    """
    proxysql_man.set_var('mysql-monitor_username', monitor_user)
    proxysql_man.set_var('mysql-monitor_password', monitor_pass)


def register_mysql_users_with_proxysql():
    pass
