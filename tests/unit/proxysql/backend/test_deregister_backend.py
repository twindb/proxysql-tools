import mock

from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLBackend


@mock.patch.object(ProxySQL, 'reload_runtime')
@mock.patch.object(ProxySQL, 'execute')
def test_deregister_backend(mock_execute, mock_runtime, proxysql):
    backend = ProxySQLMySQLBackend('foo', hostgroup_id=10, port=3307)
    proxysql.deregister_backend(backend)
    query = "DELETE FROM mysql_servers WHERE hostgroup_id=10 AND hostname='foo' AND port=3307"
    mock_execute.assert_called_once_with(query)
    mock_runtime.assert_called_once_with()
