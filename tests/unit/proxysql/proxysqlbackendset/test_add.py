from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend
from proxysql_tools.proxysql.proxysqlbackendset import ProxySQLMySQLBackendSet


def test_add():
    bs = ProxySQLMySQLBackendSet()
    be = ProxySQLMySQLBackend('foo')
    bs.add(be)
    assert len(bs) == 1
    assert be in bs


def test_two_sets_equal():
    bs_a = ProxySQLMySQLBackendSet()
    bs_b = ProxySQLMySQLBackendSet()
    be = ProxySQLMySQLBackend('foo')
    bs_a.add(be)
    bs_b.add(be)
    assert bs_a == bs_b
