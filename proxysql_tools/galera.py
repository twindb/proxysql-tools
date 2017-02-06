from proxysql_tools.managers.galera_manager import GaleraManager
from proxysql_tools.managers.proxysql_manager import ProxySQLManager


def register_cluster_with_proxysql(proxy_host, proxy_port, proxy_admin_user,
                                   proxy_admin_pass, hostgroup_id_writer,
                                   hostgroup_id_reader, cluster_host,
                                   cluster_port, cluster_user, cluster_pass):
    galera_man = GaleraManager(cluster_host, cluster_port, cluster_user,
                               cluster_pass)
    galera_man.discover_cluster_nodes()

    proxysql_man = ProxySQLManager(proxy_host, proxy_port, proxy_admin_user,
                                   proxy_admin_pass)

    # Now let's remove all the nodes defined in the writer hostgroup that
    # are not part of this cluster.
    backends_writer_hostgroup = \
        proxysql_man.fetch_mysql_backends(hostgroup_id_writer)
    for backend in backends_writer_hostgroup:
        # We found a matching cluster node so we will continue.
        for node in galera_man.nodes:
            if node.host == backend.hostname and node.port == backend.port:
                continue

        proxysql_man.deregister_mysql_backend(
            hostgroup_id_writer, backend.hostname, backend.port)

        # Remove the backend from list of writer backends as well.
        backends_writer_hostgroup.remove(backend)

    # Next let's remove all the nodes defined in the reader hostgroup that
    # are not part of this cluster.
    backends_reader_hostgroup = \
        proxysql_man.fetch_mysql_backends(hostgroup_id_reader)
    for backend in backends_reader_hostgroup:
        if node.host == backend.hostname and node.port == backend.port:
            continue

        proxysql_man.deregister_mysql_backend(
            hostgroup_id_reader, backend.hostname, backend.port)

        # Remove the backend from list of reader backends as well.
        backends_reader_hostgroup.remove(backend)


def register_mysql_users_with_proxysql():
    pass
