import pytest

from proxysql_tools.managers.galera_manager import GaleraManager
from tests.conftest import PXC_ROOT_PASSWORD
from tests.library import docker_client, eventually


@pytest.fixture
def galera_manager(percona_xtradb_cluster):
    client = docker_client()

    cluster_node = percona_xtradb_cluster[0]

    for node in percona_xtradb_cluster:
        container_info = client.containers.get(node['id'])
        assert container_info.status == 'running'

    manager = GaleraManager(cluster_node_host='127.0.0.1',
                            cluster_node_port=cluster_node['mysql_port'],
                            user='root', password=PXC_ROOT_PASSWORD)

    def check_started():
        manager.discover_cluster_nodes()

        with manager.nodes[0].get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT 1')

        return True

    # Allow the first cluster node to startup completely, while the other two
    # nodes in the cluster can keep starting up.
    eventually(check_started, retries=15, sleep_time=4)

    return manager


def test__can_connect_to_galera_node(galera_manager):
    with galera_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT VERSION() AS version")
            result = cursor.fetchone()

            version = result['version']

    assert "5.7" in version
