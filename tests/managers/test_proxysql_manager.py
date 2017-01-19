from proxysql_tools.entities.proxysql import BACKEND_STATUS_OFFLINE_SOFT, BACKEND_STATUS_OFFLINE_HARD
from tests.conftest import get_mysql_backend


def test__can_connect_to_proxysql_admin_interface(proxysql_manager):
    with proxysql_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            sql = "SELECT variable_value AS version FROM global_variables WHERE variable_name='mysql-server_version'"
            cursor.execute(sql)
            result = cursor.fetchone()

            version = result['version']

    assert version == "5.5.30"


def test__can_register_mysql_backend(proxysql_manager):
    backend = get_mysql_backend('db01')

    assert proxysql_manager.register_mysql_backend(backend.hostgroup_id, backend.hostname, backend.port)

    with proxysql_manager.get_connection() as conn:
        assert proxysql_manager.is_mysql_backend_registered(backend, conn)


def test__fetch_mysql_backends(proxysql_manager):
    backend = get_mysql_backend('db01')

    assert proxysql_manager.register_mysql_backend(backend.hostgroup_id, backend.hostname, backend.port)

    backends_list = proxysql_manager.fetch_mysql_backends()
    assert len(backends_list) == 1
    assert backends_list.pop().hostname == 'db01'


def test__update_mysql_backend_status(proxysql_manager):
    backend = get_mysql_backend('db01')

    proxysql_manager.register_mysql_backend(backend.hostgroup_id, backend.hostname, backend.port)

    assert proxysql_manager.update_mysql_backend_status(backend.hostgroup_id, backend.hostname,
                                                        backend.port, BACKEND_STATUS_OFFLINE_SOFT)

    backends_list = proxysql_manager.fetch_mysql_backends()
    assert backends_list.pop().status == BACKEND_STATUS_OFFLINE_SOFT


def test__mysql_backend_can_be_deregistered(proxysql_manager):
    backend = get_mysql_backend('db01')

    proxysql_manager.register_mysql_backend(backend.hostgroup_id, backend.hostname, backend.port)

    assert proxysql_manager.deregister_mysql_backend(backend.hostgroup_id, backend.hostname, backend.port)
    backends_list = proxysql_manager.fetch_mysql_backends()

    assert backends_list.pop().status == BACKEND_STATUS_OFFLINE_HARD
