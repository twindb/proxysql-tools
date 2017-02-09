from proxysql_tools.galera import register_cluster_with_proxysql
from proxysql_tools.managers.galera_manager import GaleraManager


def test__can_register_cluster_with_proxysql(percona_xtradb_cluster_node,
                                             proxysql_manager):
    hostgroup_writer = 10
    hostgroup_reader = 11

    # To start off there should be no nodes in the hostgroups
    assert len(proxysql_manager.fetch_mysql_backends(hostgroup_writer)) == 0
    assert len(proxysql_manager.fetch_mysql_backends(hostgroup_reader)) == 0

    galera_man = GaleraManager(percona_xtradb_cluster_node.host,
                               percona_xtradb_cluster_node.port,
                               percona_xtradb_cluster_node.username,
                               percona_xtradb_cluster_node.password)
    galera_man.discover_cluster_nodes()

    # Validate that there is one healthy node in the cluster
    assert len(galera_man.nodes) == 1

    ret = register_cluster_with_proxysql(proxysql_manager.host,
                                         proxysql_manager.port,
                                         proxysql_manager.user,
                                         proxysql_manager.password,
                                         hostgroup_writer, hostgroup_reader,
                                         percona_xtradb_cluster_node.host,
                                         percona_xtradb_cluster_node.port,
                                         percona_xtradb_cluster_node.username,
                                         percona_xtradb_cluster_node.password)

    assert ret == True

    writer_backends = proxysql_manager.fetch_mysql_backends(hostgroup_writer)
    reader_backends = proxysql_manager.fetch_mysql_backends(hostgroup_reader)

    assert len(writer_backends) == len(reader_backends) == 1
    assert writer_backends[0].hostname == reader_backends[0].hostname
    assert writer_backends[0].port == reader_backends[0].port
    assert writer_backends[0].status == reader_backends[0].status
