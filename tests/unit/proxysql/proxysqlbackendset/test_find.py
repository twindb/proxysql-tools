import pytest

from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound
from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend
from proxysql_tools.proxysql.proxysqlbackendset import ProxySQLMySQLBackendSet


def test_find_raises():
    bs = ProxySQLMySQLBackendSet()
    with pytest.raises(ProxySQLBackendNotFound):
        bs.find('foo')


def test_find():
    bs = ProxySQLMySQLBackendSet()
    be = ProxySQLMySQLBackend('foo')
    bs.add(be)

    assert bs.find('foo') == be
