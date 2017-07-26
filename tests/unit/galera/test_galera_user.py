import mock
import pytest

from proxysql_tools.galera.user import get_users, create_user, change_password, delete_user, modify_user
from proxysql_tools.proxysql.exceptions import ProxySQLUserNotFound
from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLUser


@mock.patch.object(ProxySQL, 'get_users')
def test_get_users(mock_users, config):
    mock_users.return_value = [ProxySQLMySQLUser()]
    get_users(config)


@mock.patch.object(ProxySQL, 'add_user')
def test_create_user(config):
    kwargs = {
        'username': 'root',
        'password': ''
    }
    create_user(config, kwargs)
    pass


@mock.patch.object(ProxySQL, 'add_user')
@mock.patch.object(ProxySQL, 'get_user')
@mock.patch('proxysql_tools.galera.user.get_encrypred_password')
def test_change_password(mock_add_user, mock_get_user, mock_enc_pwd, config):
    mock_get_user.return_value = ProxySQLMySQLUser()
    change_password(config, 'root', '1235')


@mock.patch.object(ProxySQL, 'add_user')
@mock.patch.object(ProxySQL, 'get_user')
@mock.patch('proxysql_tools.galera.user.get_encrypred_password')
def test_change_password_raise(mock_add_user, mock_get_user, mock_enc_pwd, config):
    mock_get_user.side_effect = ProxySQLUserNotFound
    with pytest.raises(ProxySQLUserNotFound):
        change_password(config, 'root', '1235')


@mock.patch.object(ProxySQL, 'delete_user')
def test_delete_user(mock_delete, config):
    delete_user(config, 'root')
    pass


@mock.patch.object(ProxySQL, 'add_user')
@mock.patch.object(ProxySQL, 'get_user')
@mock.patch('proxysql_tools.galera.user.parse_user_arguments')
def test_modify_user(mock_add_user, mock_get_user, mock_parser, config):
    mock_get_user.return_value = ProxySQLMySQLUser()
    mock_parser.return_value = {'foo': 'bar'}
    modify_user(config, 'test', {})
    mock_parser.assert_called_once()
    mock_add_user.assert_called_once()
    mock_get_user.assert_called_once()
