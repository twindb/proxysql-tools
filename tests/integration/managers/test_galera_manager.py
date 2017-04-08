from proxysql_tools.entities.galera import LOCAL_STATE_SYNCED
from proxysql_tools.managers.galera_manager import GaleraManager
from tests.integration.conftest import PXC_ROOT_PASSWORD, PXC_MYSQL_PORT
from tests.integration.library import wait_for_cluster_nodes_to_become_healthy


def test__can_connect_to_galera_node(percona_xtradb_cluster_node):
    with percona_xtradb_cluster_node.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT VERSION() AS version")
            result = cursor.fetchone()

            version = result['version']

    assert "5.7" in version


def test__galera_node_can_refresh_its_state(percona_xtradb_cluster_node):
    percona_xtradb_cluster_node.refresh_state()

    assert percona_xtradb_cluster_node.local_state == LOCAL_STATE_SYNCED
    assert percona_xtradb_cluster_node.cluster_status == 'primary'


def test__galera_manager_can_discover_nodes(percona_xtradb_cluster_three_node):
    wait_for_cluster_nodes_to_become_healthy(percona_xtradb_cluster_three_node)

    manager = GaleraManager(percona_xtradb_cluster_three_node[0]['ip'],
                            PXC_MYSQL_PORT, 'root', PXC_ROOT_PASSWORD)

    manager.discover_cluster_nodes()

    assert len(manager.nodes) == 3

    cluster_state_uuid = manager.nodes[0].cluster_state_uuid
    cluster_name = manager.nodes[0].cluster_name
    for cluster_node in manager.nodes[1:]:
        assert cluster_node.cluster_state_uuid == cluster_state_uuid
        assert cluster_node.cluster_name == cluster_name
