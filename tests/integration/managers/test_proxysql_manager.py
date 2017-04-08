import pytest

from proxysql_tools.entities.proxysql import (
    BACKEND_STATUS_OFFLINE_SOFT,
    BACKEND_STATUS_OFFLINE_HARD,
    ProxySQLMySQLBackend,
    ProxySQLMySQLUser
)
from proxysql_tools.managers.proxysql_manager import (
    ProxySQLManager,
    ProxySQLAdminConnectionError
)


def test__can_connect_to_proxysql_admin_interface(proxysql_manager):
    with proxysql_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            sql = ("SELECT variable_value AS version FROM global_variables "
                   "WHERE variable_name='mysql-server_version'")
            cursor.execute(sql)
            result = cursor.fetchone()

            version = result['version']

    assert version == "5.5.30"


def test__ping_successful_on_successful_connection(proxysql_manager):
    assert proxysql_manager.ping()


def test__ping_raises_exception_on_connection_failure():
    proxysql_man = ProxySQLManager(host='172.0.0.1', port=10000,
                                   user='user_error', password='pass_error')

    with pytest.raises(ProxySQLAdminConnectionError):
        proxysql_man.ping()


def test__can_register_mysql_backend(proxysql_manager):
    backend = get_mysql_backend('db01')

    assert proxysql_manager.register_backend(
        backend.hostgroup_id, backend.hostname, backend.port)

    with proxysql_manager.get_connection() as conn:
        assert proxysql_manager.is_mysql_backend_registered(backend, conn)


def test__fetch_mysql_backends(proxysql_manager):
    backend = get_mysql_backend('db01')

    assert proxysql_manager.register_backend(
        backend.hostgroup_id, backend.hostname, backend.port)

    backends_list = proxysql_manager.fetch_backends()
    assert len(backends_list) == 1
    assert backends_list.pop().hostname == 'db01'


def test__fetch_mysql_backends_belonging_to_hostgroup(proxysql_manager):
    backend = get_mysql_backend('db01')
    assert proxysql_manager.register_backend(
        backend.hostgroup_id, backend.hostname, backend.port)

    backend = get_mysql_backend('db02', hostgroup_id=200)
    assert proxysql_manager.register_backend(
        backend.hostgroup_id, backend.hostname, backend.port)

    backends_list = proxysql_manager.fetch_backends(hostgroup_id=200)
    assert len(backends_list) == 1
    assert backends_list.pop().hostname == 'db02'


def test__update_mysql_backend_status(proxysql_manager):
    backend = get_mysql_backend('db01')

    proxysql_manager.register_backend(
        backend.hostgroup_id, backend.hostname, backend.port)

    assert proxysql_manager.update_mysql_backend_status(
        backend.hostgroup_id, backend.hostname, backend.port,
        BACKEND_STATUS_OFFLINE_SOFT)

    backends_list = proxysql_manager.fetch_backends()
    assert backends_list.pop().status == BACKEND_STATUS_OFFLINE_SOFT


def test__mysql_backend_can_be_deregistered(proxysql_manager):
    backend = get_mysql_backend('db01')

    proxysql_manager.register_backend(
        backend.hostgroup_id, backend.hostname, backend.port)

    assert proxysql_manager.deregister_backend(
        backend.hostgroup_id, backend.hostname, backend.port)
    backends_list = proxysql_manager.fetch_backends()

    assert backends_list.pop().status == BACKEND_STATUS_OFFLINE_HARD


def test__can_register_mysql_user(proxysql_manager):
    user = get_mysql_user('ot')

    assert proxysql_manager.register_mysql_user(user.username, user.password,
                                                user.default_hostgroup)

    with proxysql_manager.get_connection() as conn:
        assert proxysql_manager.is_mysql_user_registered(user, conn)


def test__fetch_mysql_users(proxysql_manager):
    user = get_mysql_user('ot')

    assert proxysql_manager.register_mysql_user(user.username, user.password,
                                                user.default_hostgroup)

    users_list = proxysql_manager.fetch_mysql_users()
    assert len(users_list) == 1
    assert users_list.pop().username == 'ot'


def test__fetch_mysql_users_with_default_hostgroup(proxysql_manager):
    user = get_mysql_user('ot')
    assert proxysql_manager.register_mysql_user(user.username, user.password,
                                                user.default_hostgroup)

    user = get_mysql_user('aleks', default_hostgroup_id=200)
    assert proxysql_manager.register_mysql_user(user.username, user.password,
                                                user.default_hostgroup)

    users_list = proxysql_manager.fetch_mysql_users(default_hostgroup_id=200)
    assert len(users_list) == 1
    assert users_list.pop().username == 'aleks'


def test__can_set_proxysql_variables(proxysql_manager):
    proxysql_manager.set_var('mysql-monitor_username', 'monitor_user')
    proxysql_manager.set_var('mysql-monitor_password', 'monitor_pass')

    proxysql_vars = proxysql_manager.get_vars()
    assert proxysql_vars['mysql-monitor_username'] == 'monitor_user'
    assert proxysql_vars['mysql-monitor_password'] == 'monitor_pass'


def get_mysql_backend(hostname, hostgroup_id=None):
    backend = ProxySQLMySQLBackend()
    backend.hostgroup_id = 10 if hostgroup_id is None else hostgroup_id
    backend.hostname = hostname
    backend.port = 10000

    return backend


def get_mysql_user(username, default_hostgroup_id=None):
    user = ProxySQLMySQLUser()
    user.username = username
    user.password = 'secret_password'
    user.default_hostgroup = 10 if default_hostgroup_id is None else \
        default_hostgroup_id

    return user
