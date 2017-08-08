from proxysql_tools.proxysql.proxysql import ProxySQL
import mock


@mock.patch.object(ProxySQL, 'reload_users')
@mock.patch.object(ProxySQL, 'execute')
def test_delete_user(mock_execute, mock_runtime, proxysql):
    proxysql.delete_user('test')
    mock_runtime.assert_called_once()
