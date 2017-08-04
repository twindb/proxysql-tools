import mock
from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLUser


@mock.patch.object(ProxySQL, 'execute')
@mock.patch.object(ProxySQL, 'reload_users')
def test_add_user(mock_reload_users, mock_execute, proxysql):
    user = ProxySQLMySQLUser(username='foo', password='')
    proxysql.add_user(user)
    mock_reload_users.assert_called_once()
