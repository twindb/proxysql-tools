import pytest
from doubles import allow, expect
from mock import MagicMock

from proxysql_tools.entities.proxysql import ProxySQLMySQLBackend
from proxysql_tools.managers.proxysql_manager import ProxySQLManager, ProxySQLMySQLBackendUnregistered


def test__deregister_backend_deletes_the_backend():
    proxysql_man = ProxySQLManager('127.0.0.1', 6032, 'username', 'password')

    backend = ProxySQLMySQLBackend({
        'hostgroup_id': 10,
        'hostname': 'dummy_host',
        'port': 3306
    })

    (allow(proxysql_man)
        .get_connection
        .and_return(MagicMock()))

    (allow(proxysql_man)
        .is_mysql_backend_registered
        .and_return(True))

    (expect(proxysql_man)
        .delete_mysql_backend
        .once())

    proxysql_man.deregister_backend(backend.hostgroup_id, backend.hostname,
                                    backend.port)


def test__deregister_backend_does_not_delete_the_backend_if_not_registered():
    proxysql_man = ProxySQLManager('127.0.0.1', 6032, 'username', 'password')

    backend = ProxySQLMySQLBackend({
        'hostgroup_id': 10,
        'hostname': 'dummy_host',
        'port': 3306
    })

    (allow(proxysql_man)
        .get_connection
        .and_return(MagicMock()))

    (allow(proxysql_man)
        .is_mysql_backend_registered
        .and_return(False))

    (expect(proxysql_man)
        .delete_mysql_backend
        .never())

    proxysql_man.deregister_backend(backend.hostgroup_id, backend.hostname,
                                    backend.port)


def test_register_backend():
    proxysql_man = ProxySQLManager('127.0.0.1', 6032, 'username', 'password')
    backend = ProxySQLMySQLBackend({
        'hostgroup_id': 10,
        'hostname': 'dummy_host',
        'port': 3306
    })

    (allow(proxysql_man)
        .get_connection
        .and_return(MagicMock()))

    (expect(proxysql_man)
        .insert_or_update_mysql_backend
        .once())

    proxysql_man.register_backend(backend.hostgroup_id, backend.hostname, backend.port)


def test_update_mysql_backend_status_if_backend_registered():
    proxysql_man = ProxySQLManager('127.0.0.1', 6032, 'username', 'password')
    backend = ProxySQLMySQLBackend({
        'hostgroup_id': 10,
        'hostname': 'dummy_host',
        'port': 3306
    })
    (allow(proxysql_man)
        .get_connection
        .and_return(MagicMock()))

    (allow(proxysql_man)
        .is_mysql_backend_registered
        .and_return(True))

    (expect(proxysql_man)
        .insert_or_update_mysql_backend
        .once())

    proxysql_man.update_mysql_backend_status(backend.hostgroup_id, backend.hostname, backend.port, "dummy_status")


def test_update_mysql_backend_status_if_backend_not_registered():
    proxysql_man = ProxySQLManager('127.0.0.1', 6032, 'username', 'password')
    backend = ProxySQLMySQLBackend({
        'hostgroup_id': 10,
        'hostname': 'dummy_host',
        'port': 3306
    })
    (allow(proxysql_man)
        .get_connection
        .and_return(MagicMock()))

    (allow(proxysql_man)
        .is_mysql_backend_registered
        .and_return(False))

    (expect(proxysql_man)
        .insert_or_update_mysql_backend
        .never())

    with pytest.raises(ProxySQLMySQLBackendUnregistered):
        proxysql_man.update_mysql_backend_status(backend.hostgroup_id, backend.hostname, backend.port, "dummy_status")


def test_register_mysql_user_if_registered():
    proxysql_man = ProxySQLManager('127.0.0.1', 6032, 'username', 'password')

    (allow(proxysql_man)
        .get_connection
        .and_return(MagicMock()))

    (allow(proxysql_man)
        .is_mysql_user_registered
        .and_return(True))

    (expect(proxysql_man)
        .insert_or_update_mysql_user
        .never())

    assert proxysql_man.register_mysql_user("user", "pwd", 1)


def test_register_mysql_user_if_not_registered():
    proxysql_man = ProxySQLManager('127.0.0.1', 6032, 'username', 'password')

    (allow(proxysql_man)
        .get_connection
        .and_return(MagicMock()))

    (allow(proxysql_man)
        .is_mysql_user_registered
        .and_return(False))

    (expect(proxysql_man)
        .insert_or_update_mysql_user
        .once())

    proxysql_man.register_mysql_user("user", "pwd", 1)
