import pytest

from proxysql_tools import setup_logging, LOG
from proxysql_tools.galera.galera_cluster import GaleraCluster
from proxysql_tools.galera.galera_node import GaleraNode
from proxysql_tools.proxysql.proxysql import ProxySQL

setup_logging(LOG, debug=True)


@pytest.fixture
def galera_node1():
    return GaleraNode('192.168.90.2', password='root')


@pytest.fixture
def galera_cluster():
    hosts = "192.168.90.2:3306,192.168.90.3:3306,192.168.90.4:3306"
    return GaleraCluster(hosts, password='root')


@pytest.fixture
def proxysql():
    return ProxySQL(host='127.0.0.1', port=6032,
                    user='admin', password='admin')
