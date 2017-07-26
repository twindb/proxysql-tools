import pytest

from proxysql_tools.galera.galera_node import GaleraNode
from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLBackend
from tests.integration.library import proxysql_tools_config


@pytest.fixture
def galera_node():
    return GaleraNode('foo', port=1234, user='bar', password='xyz')


@pytest.fixture
def proxysql_mysql_backend():
    return ProxySQLMySQLBackend('127.0.0.1')


@pytest.fixture
def config():
    proxysql_instance = ProxySQL()
    data = proxysql_tools_config(proxysql_instance, '127.0.0.1', '3306',
                                 'user', 'pass', 10, 11, 'monitor',
                                 'monitor')
    return data
