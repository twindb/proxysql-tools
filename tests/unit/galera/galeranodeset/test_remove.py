import pytest

from proxysql_tools.galera.exceptions import GaleraClusterNodeNotFound
from proxysql_tools.galera.galera_node import GaleraNode
from proxysql_tools.galera.galeranodeset import GaleraNodeSet


def test_remove():
    bs = GaleraNodeSet()
    be = GaleraNode('foo')
    bs.add(be)
    bs.remove(be)
    assert len(bs) == 0
    assert be not in bs


def test_remove_non_existing_raises():
    bs = GaleraNodeSet()
    be = GaleraNode('foo')
    with pytest.raises(GaleraClusterNodeNotFound):
        bs.remove(be)
