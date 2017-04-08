from proxysql_tools.entities.galera import GaleraConfig
from proxysql_tools.entities.proxysql import (
    BACKEND_STATUS_ONLINE,
    ProxySQLConfig
)
from proxysql_tools.galera import register_cluster_with_proxysql
from proxysql_tools.managers.galera_manager import GaleraManager
from tests.integration.conftest import PXC_MYSQL_PORT, PXC_ROOT_PASSWORD
from tests.integration.library import wait_for_cluster_nodes_to_become_healthy


def test__register_cluster_with_proxysql_is_idempotent(
        percona_xtradb_cluster_node, proxysql_manager):
    hostgroup_writer = 10
    hostgroup_reader = 11

    # Setup the config objects
    proxy_config = ProxySQLConfig({
        'host': proxysql_manager.host,
        'admin_port': proxysql_manager.port,
        'admin_username': proxysql_manager.user,
        'admin_password': proxysql_manager.password,
        'monitor_username': 'monitor',
        'monitor_password': 'monitor'
    })
    galera_config = GaleraConfig({
        'writer_hostgroup_id': hostgroup_writer,
        'reader_hostgroup_id': hostgroup_reader,
        'cluster_host': '%s:%s' %
                        (percona_xtradb_cluster_node.host,
                         percona_xtradb_cluster_node.port),
        'cluster_username': percona_xtradb_cluster_node.username,
        'cluster_password': percona_xtradb_cluster_node.password,
        'load_balancing_mode': 'singlewriter'
    })

    # Do an initial registration of the cluster
    assert register_cluster_with_proxysql(proxy_config, galera_config)

    writer_backends = proxysql_manager.fetch_backends(hostgroup_writer)
    reader_backends = proxysql_manager.fetch_backends(hostgroup_reader)

    # We try to register again and check that registration is idempotent
    assert register_cluster_with_proxysql(proxy_config, galera_config)

    assert proxysql_manager.fetch_backends(hostgroup_writer)[0].hostname == writer_backends[0].hostname  # NOQA
    assert proxysql_manager.fetch_backends(hostgroup_reader)[0].hostname == reader_backends[0].hostname  # NOQA


def test__register_cluster_with_proxysql_removes_incorrect_nodes(
        percona_xtradb_cluster_three_node,
        percona_xtradb_cluster_node, proxysql_manager):
    hostgroup_writer = 10
    hostgroup_reader = 11

    # Let's wait for our 3-node cluster to become healthy first
    wait_for_cluster_nodes_to_become_healthy(percona_xtradb_cluster_three_node)

    # Setup the config objects
    proxy_config = ProxySQLConfig({
        'host': proxysql_manager.host,
        'admin_port': proxysql_manager.port,
        'admin_username': proxysql_manager.user,
        'admin_password': proxysql_manager.password,
        'monitor_username': 'monitor',
        'monitor_password': 'monitor'
    })
    galera_config = GaleraConfig({
        'writer_hostgroup_id': hostgroup_writer,
        'reader_hostgroup_id': hostgroup_reader,
        'cluster_host': '%s:%s' %
                        (percona_xtradb_cluster_node.host,
                         percona_xtradb_cluster_node.port),
        'cluster_username': percona_xtradb_cluster_node.username,
        'cluster_password': percona_xtradb_cluster_node.password,
        'load_balancing_mode': 'singlewriter'
    })

    # Register the node first from the 1-node cluster
    assert register_cluster_with_proxysql(proxy_config, galera_config)

    # Now we register the nodes from the three node cluster
    galera_man = GaleraManager(percona_xtradb_cluster_three_node[0]['ip'],
                               PXC_MYSQL_PORT, 'root', PXC_ROOT_PASSWORD)

    # Setup the config object again
    galera_config = GaleraConfig({
        'writer_hostgroup_id': hostgroup_writer,
        'reader_hostgroup_id': hostgroup_reader,
        'cluster_host': '%s:%s' % (galera_man.host, galera_man.port),
        'cluster_username': galera_man.user,
        'cluster_password': galera_man.password,
        'load_balancing_mode': 'singlewriter'
    })

    assert register_cluster_with_proxysql(proxy_config, galera_config)

    # There should be one writer backend and two reader backends.
    # Also validate the backends to make sure they are part of the 3-node
    # cluster.
    galera_man.discover_cluster_nodes()

    writer_backends = [b for b in
                       proxysql_manager.fetch_backends(hostgroup_writer)
                       if b.status == BACKEND_STATUS_ONLINE]
    reader_backends = [b for b in
                       proxysql_manager.fetch_backends(hostgroup_reader)
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
