import pytest
import mock

from proxysql_tools.proxysql.exceptions import ProxySQLUserNotFound
from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLUser


@mock.patch.object(ProxySQL, 'execute')
def test_get_user(mock_execute, proxysql):
    user = ProxySQLMySQLUser(username='foo', password='bar')
    proxysql.get_user('test')
    query = "SELECT * FROM mysql_users WHERE username = 'test'"
    mock_execute.assert_called_once_with(query)


@mock.patch.object(ProxySQL, 'execute')
def test_get_user_if_user_does_not_exist(mock_execute, proxysql):
    user = ProxySQLMySQLUser(username='foo', password='bar')
    mock_execute.return_value = []
    with pytest.raises(ProxySQLUserNotFound):
        proxysql.get_user('test')

    query = "SELECT * FROM mysql_users WHERE username = 'test'"
    mock_execute.assert_called_once_with(query)

@pytest.mark.parametrize('response',[
    (
        [{
            u'username': 'foo',
            u'password': '',
            u'active': False,
            u'use_ssl': False,
            u'default_hostgroup': 0,
            u'default_schema': 'information_schema',
            u'schema_locked': False,
            u'transaction_persistent': False,
            u'fast_forward': False,
            u'backend': False,
            u'frontend': True,
            u'max_connections': '10000'
        }]
    )
])
@mock.patch.object(ProxySQL, 'execute')
def test_get_users(mock_execute, proxysql, response):
    query = "SELECT * FROM mysql_users;"
    mock_execute.return_value = response
    proxysql.get_users()
    mock_execute.assert_called_once_with(query)
