from doubles import allow, expect
from mock import MagicMock

from proxysql_tools.entities.proxysql import ProxySQLMySQLBackend
from proxysql_tools.managers.proxysql_manager import ProxySQLManager


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
