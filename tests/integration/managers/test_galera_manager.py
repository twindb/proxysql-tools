import pytest

from proxysql_tools.entities.galera import GaleraNode, LOCAL_STATE_SYNCED
from proxysql_tools.managers.galera_manager import GaleraManager
from tests.conftest import PXC_ROOT_PASSWORD, PXC_MYSQL_PORT
from tests.library import eventually


@pytest.fixture
def percona_xtradb_cluster_node(percona_xtradb_cluster_one_node):
    node = GaleraNode({
        'host': percona_xtradb_cluster_one_node[0]['ip'],
        'username': 'root',
        'password': PXC_ROOT_PASSWORD
    })

    def check_started():
        with node.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT 1')

        return True

    # Allow the cluster node to startup completely.
    eventually(check_started, retries=15, sleep_time=4)

    return node


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
    def check_started():
        for container_info in percona_xtradb_cluster_three_node:
            node = GaleraNode({
                'host': container_info['ip'],
                'username': 'root',
                'password': PXC_ROOT_PASSWORD
            })
            with node.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT 1')

        return True

    # Allow all the cluster nodes to startup.
    eventually(check_started, retries=20, sleep_time=4)

    manager = GaleraManager(percona_xtradb_cluster_three_node[0]['ip'],
                            PXC_MYSQL_PORT, 'root', PXC_ROOT_PASSWORD)

    manager.discover_cluster_nodes()

    assert len(manager.nodes) == 3

    cluster_state_uuid = manager.nodes[0].cluster_state_uuid
    cluster_name = manager.nodes[0].cluster_name
    for cluster_node in manager.nodes[1:]:
        assert cluster_node.cluster_state_uuid == cluster_state_uuid
        assert cluster_node.cluster_name == cluster_name