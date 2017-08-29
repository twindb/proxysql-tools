from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend
from proxysql_tools.proxysql.proxysqlbackendset import ProxySQLMySQLBackendSet


def test_eq_empty():
    assert ProxySQLMySQLBackendSet() == ProxySQLMySQLBackendSet()


def test_eq_one():
    set1 = ProxySQLMySQLBackendSet()
    set2 = ProxySQLMySQLBackendSet()

    set1.add(ProxySQLMySQLBackend('foo'))
    set2.add(ProxySQLMySQLBackend('foo'))

    assert set1 == set2


def test_neq():
    set1 = ProxySQLMySQLBackendSet()
    set2 = ProxySQLMySQLBackendSet()
    set1.add(ProxySQLMySQLBackend('foo'))

    assert set1 != set2


def test_neq_set_and_backend():
    set1 = ProxySQLMySQLBackendSet()
    set1.add(ProxySQLMySQLBackend('foo'))

    assert set1 != ProxySQLMySQLBackend('foo')
