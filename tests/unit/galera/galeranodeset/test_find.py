import pytest

from proxysql_tools.galera.exceptions import GaleraClusterNodeNotFound
from proxysql_tools.galera.galera_node import GaleraNode
from proxysql_tools.galera.galeranodeset import GaleraNodeSet


def test_find_raises():
    bs = GaleraNodeSet()
    with pytest.raises(GaleraClusterNodeNotFound):
        bs.find(host='foo')


def test_find_host():
    bs = GaleraNodeSet()
    be = GaleraNode('foo')
    bs.add(be)

    assert bs.find(host='foo')[0] == be
