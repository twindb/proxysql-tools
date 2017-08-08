import mock
import pytest

from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound
from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLBackend


@pytest.mark.parametrize('response, backend',[
    (
        [{
            u'status': 'ONLINE',
            u'comment': '',
            u'compression': '0',
            u'weight': '1',
            u'hostname': '192.168.90.2',
            u'hostgroup_id': '10',
            u'use_ssl': '0',
            u'max_replication_lag': '0',
            u'port': '3306',
            u'max_latency_ms': '0',
            u'max_connections': '10000'
        }]
        ,
        ProxySQLMySQLBackend('192.168.90.2', hostgroup_id=10, port=3306)
    )
])
@mock.patch.object(ProxySQL, 'execute')
def test_find_backends(mock_execute, proxysql, response, backend):
    mock_execute.return_value = response
    assert proxysql.find_backends(10)[0] == backend


@pytest.mark.parametrize('response, backend',[
    (
        [{
            u'status': 'ONLINE',
            u'comment': '',
            u'compression': '0',
            u'weight': '1',
            u'hostname': '192.168.90.2',
            u'hostgroup_id': '10',
            u'use_ssl': '0',
            u'max_replication_lag': '0',
            u'port': '3306',
            u'max_latency_ms': '0',
            u'max_connections': '10000'
        }]
        ,
        ProxySQLMySQLBackend('192.168.90.2', hostgroup_id=10, port=3306)
    ),
    (
        [{
            u'status': 'ONLINE',
            u'comment': '',
            u'compression': '0',
            u'weight': '1',
            u'hostname': '192.168.90.1',
            u'hostgroup_id': '11',
            u'use_ssl': '0',
            u'max_replication_lag': '0',
            u'port': '3306',
            u'max_latency_ms': '0',
            u'max_connections': '10000'
        }]
        ,
        ProxySQLMySQLBackend('192.168.90.1', hostgroup_id=11, port=3306)
    )
])
@mock.patch.object(ProxySQL, 'execute')
def test_find_backends_without_hostgroup(mock_execute, proxysql, response, backend):
    mock_execute.return_value = response
    assert proxysql.find_backends()[0] == backend


@pytest.mark.parametrize('response, backend',[
    (
        [{
            u'status': 'ONLINE',
            u'comment': '',
            u'compression': '0',
            u'weight': '1',
            u'hostname': '192.168.90.2',
            u'hostgroup_id': '10',
            u'use_ssl': '0',
            u'max_replication_lag': '0',
            u'port': '3306',
            u'max_latency_ms': '0',
            u'max_connections': '10000'
        }]
        ,
        ProxySQLMySQLBackend('192.168.90.2', hostgroup_id=10, port=3306)
    ),
    (
        [{
            u'status': 'OFFLINE',
            u'comment': '',
            u'compression': '0',
            u'weight': '1',
            u'hostname': '192.168.90.1',
            u'hostgroup_id': '11',
            u'use_ssl': '0',
            u'max_replication_lag': '0',
            u'port': '3306',
            u'max_latency_ms': '0',
            u'max_connections': '10000'
        }]
        ,
        ProxySQLMySQLBackend('192.168.90.1', hostgroup_id=11, port=3306, status='OFFLINE')
    )
])
@mock.patch.object(ProxySQL, 'execute')
def test_find_backends_without_status(mock_execute, proxysql, response, backend):
    mock_execute.return_value = response
    assert proxysql.find_backends()[0] == backend


@mock.patch.object(ProxySQL, 'execute')
def test_find_backends_raises(mock_execute, proxysql):
    mock_execute.return_value = ()
    with pytest.raises(ProxySQLBackendNotFound):
        proxysql.find_backends(10)
