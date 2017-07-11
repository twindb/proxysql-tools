import mock
import pytest

from proxysql_tools.galera.user import get_users, create_user, change_password, delete_user
from proxysql_tools.proxysql.exceptions import ProxySQLUserNotFound
from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLUser


@mock.patch.object(ProxySQL, 'get_users')
def test_get_users(mock_users, config):
    mock_users.return_value = [ProxySQLMySQLUser()]
    get_users(config)

@mock.patch.object(ProxySQL, 'add_user')
def test_create_user(config):
    kwargs = {
        'user': 'root'
    }
    create_user(config, kwargs)
    pass


@mock.patch.object(ProxySQL, 'add_user')
@mock.patch.object(ProxySQL, 'get_user')
def test_change_password(mock_add_user, mock_get_user, config):
    change_password(config, 'root', '1235')


@mock.patch.object(ProxySQL, 'add_user')
@mock.patch.object(ProxySQL, 'get_user')
def test_change_password_raise(mock_add_user, mock_get_user, config):
    mock_get_user.side_effect = ProxySQLUserNotFound
    with pytest.raises(ProxySQLUserNotFound):
        change_password(config, 'root', '1235')


@mock.patch.object(ProxySQL, 'delete_user')
def test_delete_user(config):
    delete_user(config, 'root')
    pass
