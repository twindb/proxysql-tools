from proxysql_tools.entities.proxysql import BACKEND_STATUS_ONLINE
from proxysql_tools.galera import register_cluster_with_proxysql
from proxysql_tools.managers.galera_manager import GaleraManager
from tests.conftest import PXC_MYSQL_PORT, PXC_ROOT_PASSWORD
from tests.library import wait_for_cluster_nodes_to_become_healthy


def test__can_register_cluster_with_proxysql(percona_xtradb_cluster_node,
                                             proxysql_manager):
    hostgroup_writer = 10
    hostgroup_reader = 11

    # To start off there should be no nodes in the hostgroups
    assert len(proxysql_manager.fetch_mysql_backends(hostgroup_writer)) == 0
    assert len(proxysql_manager.fetch_mysql_backends(hostgroup_reader)) == 0

    galera_man = GaleraManager(
        percona_xtradb_cluster_node.host, percona_xtradb_cluster_node.port,
        percona_xtradb_cluster_node.username,
        percona_xtradb_cluster_node.password
    )
    galera_man.discover_cluster_nodes()

    # Validate that there is one healthy node in the cluster
    assert len(galera_man.nodes) == 1

    ret = register_cluster_with_proxysql(
        proxysql_manager.host, proxysql_manager.port, proxysql_manager.user,
        proxysql_manager.password, hostgroup_writer, hostgroup_reader,
        percona_xtradb_cluster_node.host, percona_xtradb_cluster_node.port,
        percona_xtradb_cluster_node.username,
        percona_xtradb_cluster_node.password
    )

    assert ret

    writer_backends = proxysql_manager.fetch_mysql_backends(hostgroup_writer)
    reader_backends = proxysql_manager.fetch_mysql_backends(hostgroup_reader)

    assert len(writer_backends) == len(reader_backends) == 1
    assert writer_backends[0].hostname == reader_backends[0].hostname
    assert writer_backends[0].port == reader_backends[0].port
    assert writer_backends[0].status == reader_backends[0].status


def test__register_cluster_with_proxysql_is_idempotent(
        percona_xtradb_cluster_node, proxysql_manager):
    hostgroup_writer = 10
    hostgroup_reader = 11

    # Do an initial registration of the cluster
    ret = register_cluster_with_proxysql(
        proxysql_manager.host, proxysql_manager.port, proxysql_manager.user,
        proxysql_manager.password, hostgroup_writer, hostgroup_reader,
        percona_xtradb_cluster_node.host, percona_xtradb_cluster_node.port,
        percona_xtradb_cluster_node.username,
        percona_xtradb_cluster_node.password
    )

    assert ret

    writer_backends = proxysql_manager.fetch_mysql_backends(hostgroup_writer)
    reader_backends = proxysql_manager.fetch_mysql_backends(hostgroup_reader)

    # We try to register again and check that registration is idempotent
    ret = register_cluster_with_proxysql(
        proxysql_manager.host, proxysql_manager.port, proxysql_manager.user,
        proxysql_manager.password, hostgroup_writer, hostgroup_reader,
        percona_xtradb_cluster_node.host, percona_xtradb_cluster_node.port,
        percona_xtradb_cluster_node.username,
        percona_xtradb_cluster_node.password
    )

    assert ret

    assert proxysql_manager.fetch_mysql_backends(hostgroup_writer)[0].hostname == writer_backends[0].hostname  # NOQA
    assert proxysql_manager.fetch_mysql_backends(hostgroup_reader)[0].hostname == reader_backends[0].hostname  # NOQA


def test__register_cluster_with_proxysql_removes_incorrect_nodes(
        percona_xtradb_cluster_three_node,
        percona_xtradb_cluster_node, proxysql_manager):
    hostgroup_writer = 10
    hostgroup_reader = 11

    # Let's wait for our 3-node cluster to become healthy first
    wait_for_cluster_nodes_to_become_healthy(percona_xtradb_cluster_three_node)

    # Register the node first from the 1-node cluster
    ret = register_cluster_with_proxysql(
        proxysql_manager.host, proxysql_manager.port, proxysql_manager.user,
        proxysql_manager.password, hostgroup_writer, hostgroup_reader,
        percona_xtradb_cluster_node.host, percona_xtradb_cluster_node.port,
        percona_xtradb_cluster_node.username,
        percona_xtradb_cluster_node.password
    )

    assert ret

    # Now we register the nodes from the three node cluster
    galera_man = GaleraManager(percona_xtradb_cluster_three_node[0]['ip'],
                               PXC_MYSQL_PORT, 'root', PXC_ROOT_PASSWORD)

    ret = register_cluster_with_proxysql(
        proxysql_manager.host, proxysql_manager.port, proxysql_manager.user,
        proxysql_manager.password, hostgroup_writer, hostgroup_reader,
        galera_man.host, galera_man.port, galera_man.user, galera_man.password
    )

    assert ret

    # There should be one writer backend and two reader backends.
    # Also validate the backends to make sure they are part of the 3-node
    # cluster.
    galera_man.discover_cluster_nodes()

    writer_backends = [b for b in
                       proxysql_manager.fetch_mysql_backends(hostgroup_writer)
                       if b.status == BACKEND_STATUS_ONLINE]
    reader_backends = [b for b in
                       proxysql_manager.fetch_mysql_backends(hostgroup_reader)
                       if b.status == BACKEND_STATUS_ONLINE]

    assert len(writer_backends) == 1
    assert len(reader_backends) == 2

    writer_nodes = [n for n in galera_man.nodes for w in writer_backends
                    if n.host == w.hostname and n.port == w.port]
    reader_nodes = [n for n in galera_man.nodes for r in reader_backends
                    if r.hostname == n.host and r.port == n.port]

    assert len(writer_nodes) == 1
    assert len(reader_nodes) == 2

    # Now we validate that the writer nodes and readers node form a disjoint
    # set
    assert len(set(writer_nodes).intersection(set(reader_nodes))) == 0
