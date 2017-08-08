import mock
import pytest

from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLBackend


@pytest.mark.parametrize('comment, query',[
    (
        'Some comment',
        "REPLACE INTO mysql_servers(`hostgroup_id`, `hostname`, `port`, `status`, `weight`, `compression`, `max_connections`, `max_replication_lag`, `use_ssl`, `max_latency_ms`, `comment`) VALUES(0, 'foo', 3306, 'ONLINE', 1, 0, 10000, 0, 0, 0, 'Some comment')"
    ),
    (
        None,
        "REPLACE INTO mysql_servers(`hostgroup_id`, `hostname`, `port`, `status`, `weight`, `compression`, `max_connections`, `max_replication_lag`, `use_ssl`, `max_latency_ms`, `comment`) VALUES(0, 'foo', 3306, 'ONLINE', 1, 0, 10000, 0, 0, 0, NULL)"
    )
])
@mock.patch.object(ProxySQL, 'reload_runtime')
@mock.patch.object(ProxySQL, 'execute')
def test_register_backend(mock_execute, mock_runtime, comment, query, proxysql):
    """

    :param mock_execute:
    :param mock_runtime:
    :param comment:
    :param query:
    :param proxysql:
    :type proxysql: ProxySQL
    """
    backend = ProxySQLMySQLBackend('foo', comment=comment)
    proxysql.register_backend(backend)
    mock_execute.assert_called_once_with(query)
    mock_runtime.assert_called_once_with()
