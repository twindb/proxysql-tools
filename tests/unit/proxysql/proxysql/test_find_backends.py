import mock
import pytest

from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound
from proxysql_tools.proxysql.proxysql import ProxySQL
from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend


# noinspection PyUnresolvedReferences
@pytest.mark.parametrize('response, backend', [
    (
        [
            {
                u'status': 'ONLINE',
                u'comment': '{ "role": "Reader", "admin_status": "ONLINE" }',
                u'compression': '0',
                u'weight': '1',
                u'hostname': '192.168.90.2',
                u'hostgroup_id': '10',
                u'use_ssl': '0',
                u'max_replication_lag': '0',
                u'port': '3306',
                u'max_latency_ms': '0',
                u'max_connections': '10000',
            }
        ],
        ProxySQLMySQLBackend('192.168.90.2',
                             hostgroup_id=10,
                             port=3306,
                             comment='{ "role": "Reader", '
                                     '"admin_status": "ONLINE" }')
    )
])
@mock.patch.object(ProxySQL, 'execute')
def test_find_backends_simple(mock_execute, proxysql, response, backend):
    mock_execute.return_value = response
    assert proxysql.find_backends(10)[0] == backend


# noinspection PyUnresolvedReferences
@mock.patch.object(ProxySQL, 'execute')
def test_find_backends_raises(mock_execute, proxysql):
    mock_execute.return_value = ()
    with pytest.raises(ProxySQLBackendNotFound):
        proxysql.find_backends(10)


# noinspection PyUnresolvedReferences
@pytest.mark.parametrize('query, response, hostgroup_id, status', [
    (
        'SELECT '
        '`hostgroup_id`, `hostname`, `port`, '
        '`status`, `weight`, `compression`, '
        '`max_connections`, `max_replication_lag`, `use_ssl`, '
        '`max_latency_ms`, `comment` '
        'FROM `mysql_servers` '
        'WHERE 1=1 AND hostgroup_id = 10 AND status = \'ONLINE\'',
        [
            {
                u'status': 'ONLINE',
                u'comment': '{ "role": "Reader", "admin_status": "ONLINE" }',
                u'compression': '0',
                u'weight': '1',
                u'hostname': '192.168.90.2',
                u'hostgroup_id': '10',
                u'use_ssl': '0',
                u'max_replication_lag': '0',
                u'port': '3306',
                u'max_latency_ms': '0',
                u'max_connections': '10000',
            }
        ],
        10,
        'ONLINE'
    ),
    (
        'SELECT '
        '`hostgroup_id`, `hostname`, `port`, '
        '`status`, `weight`, `compression`, '
        '`max_connections`, `max_replication_lag`, `use_ssl`, '
        '`max_latency_ms`, `comment` '
        'FROM `mysql_servers` '
        'WHERE 1=1 AND hostgroup_id = 10',
        [
            {
                u'status': 'ONLINE',
                u'comment': '{ "role": "Reader", "admin_status": "ONLINE" }',
                u'compression': '0',
                u'weight': '1',
                u'hostname': '192.168.90.2',
                u'hostgroup_id': '10',
                u'use_ssl': '0',
                u'max_replication_lag': '0',
                u'port': '3306',
                u'max_latency_ms': '0',
                u'max_connections': '10000',
            }
        ],
        10,
        None
    ),
    (
        'SELECT '
        '`hostgroup_id`, `hostname`, `port`, '
        '`status`, `weight`, `compression`, '
        '`max_connections`, `max_replication_lag`, `use_ssl`, '
        '`max_latency_ms`, `comment` '
        'FROM `mysql_servers` WHERE 1=1',
        [
            {
                u'status': 'ONLINE',
                u'comment': '{ "role": "Reader", "admin_status": "ONLINE" }',
                u'compression': '0',
                u'weight': '1',
                u'hostname': '192.168.90.2',
                u'hostgroup_id': '10',
                u'use_ssl': '0',
                u'max_replication_lag': '0',
                u'port': '3306',
                u'max_latency_ms': '0',
                u'max_connections': '10000',
            }
        ],
        None,
        None
    ),
])
@mock.patch.object(ProxySQL, 'execute')
def test_find_backends(mock_execute, proxysql, query, response,
                       hostgroup_id, status):
    mock_execute.return_value = response
    proxysql.find_backends(hostgroup_id=hostgroup_id,
                           status=status)
    mock_execute.assert_called_once_with(query)
