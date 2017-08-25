import pytest

from proxysql_tools.galera.galera_cluster import GaleraCluster
from proxysql_tools.galera.galera_node import GaleraNode
from proxysql_tools.galera.galeranodeset import GaleraNodeSet


def test_nodes():
    gc = GaleraCluster('foo:1')
    node_set = GaleraNodeSet()
    node_set.add(GaleraNode('foo', 1))
    assert gc.nodes == node_set


@pytest.mark.parametrize('cluster_host, result', [
    (
        "192.168.90.2:3306,192.168.90.3:3306,192.168.90.4:3306",
        [
            ('192.168.90.2', 3306),
            ('192.168.90.3', 3306),
            ('192.168.90.4', 3306),
        ]
    )
])
def test_split_cluster_host(cluster_host, result):
    assert GaleraCluster(cluster_host)._split_cluster_host(cluster_host) == result
