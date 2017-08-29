from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend
from proxysql_tools.proxysql.proxysqlbackendset import ProxySQLMySQLBackendSet


def test_add_set():
    bs_a = ProxySQLMySQLBackendSet()
    bs_b = ProxySQLMySQLBackendSet()

    be = ProxySQLMySQLBackend('foo')
    bs_a.add(be)

    bs_b.add_set(bs_a)
    assert len(bs_b) == 1
    assert be in bs_b


def test_subset_in_set():

    bs_a = ProxySQLMySQLBackendSet()
    bs_b = ProxySQLMySQLBackendSet()

    be = ProxySQLMySQLBackend('foo')
    bs_a.add(be)

    bs_b.add_set(bs_a)

    assert bs_a in bs_b

    bs_b.add(ProxySQLMySQLBackend('bar'))

    assert bs_b not in bs_a
