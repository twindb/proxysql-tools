import pytest
from pymysql import OperationalError

from proxysql_tools.proxysql.proxysql import ProxySQLMySQLBackend


def test_proxysql_ping(proxysql):
    assert proxysql.ping()


def test_execute_select(proxysql):
    assert proxysql.execute('SELECT 1') == [{'1': '1'}]
    assert proxysql.execute('SHOW TABLES') == [
        {u'tables': 'global_variables'},
        {u'tables': 'mysql_collations'},
        {u'tables': 'mysql_query_rules'},
        {u'tables': 'mysql_replication_hostgroups'},
        {u'tables': 'mysql_servers'},
        {u'tables': 'mysql_users'},
        {u'tables': 'runtime_global_variables'},
        {u'tables': 'runtime_mysql_query_rules'},
        {u'tables': 'runtime_mysql_replication_hostgroups'},
        {u'tables': 'runtime_mysql_servers'},
        {u'tables': 'runtime_mysql_users'},
        {u'tables': 'runtime_scheduler'},
        {u'tables': 'scheduler'}
    ]


def test_execute_set(proxysql):
    assert proxysql.execute('set mysql-wait_timeout=28800000') == ()


def test_execute_wrong_sql(proxysql):
    with pytest.raises(OperationalError):
        assert proxysql.execute('aaa')


def test_register_backend(proxysql):
    backend = ProxySQLMySQLBackend('192.168.90.2', hostgroup_id=10)
    proxysql.register_backend(backend)
    proxysql.deregister_backend(backend)


def test_writer(proxysql):
    backend = ProxySQLMySQLBackend('192.168.90.2', hostgroup_id=10)
    proxysql.register_backend(backend)
    assert backend == proxysql.find_backends(10)[0]
