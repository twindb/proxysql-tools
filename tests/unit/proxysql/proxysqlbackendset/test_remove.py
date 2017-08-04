import pytest

from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound
from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend
from proxysql_tools.proxysql.proxysqlbackendset import ProxySQLMySQLBackendSet


def test_remove():
    bs = ProxySQLMySQLBackendSet()
    be = ProxySQLMySQLBackend('foo')
    bs.add(be)
    bs.remove(be)
    assert len(bs) == 0
    assert be not in bs


def test_remove_non_existing_raises():
    bs = ProxySQLMySQLBackendSet()
    be = ProxySQLMySQLBackend('foo')
    with pytest.raises(ProxySQLBackendNotFound):
        bs.remove(be)
