import time

from proxysql_tools.entities.proxysql import ProxySQLMySQLBackend
from proxysql_tools.managers.proxysql_manager import ProxySQLManager
from tests.conftest import PROXYSQL_ADMIN_PORT, PROXYSQL_ADMIN_USER, PROXYSQL_ADMIN_PASSWORD


def test__can_connect_to_proxysql_admin_interface(proxysql_container):
    assert proxysql_container.status == 'running'

    container_ip = '127.0.0.1'

    admin_port_bindings = proxysql_container.attrs['NetworkSettings']['Ports']['%s/tcp' % PROXYSQL_ADMIN_PORT]
    admin_port = int(admin_port_bindings.pop()['HostPort'])
    assert admin_port

    manager = ProxySQLManager(host=container_ip, port=admin_port, user=PROXYSQL_ADMIN_USER,
                              password=PROXYSQL_ADMIN_PASSWORD)

    # Allow ProxySQL to startup completely. The problem is that ProxySQL starts listening to the admin port
    # before it has initialized completely which causes the test to fail with the exception:
    # OperationalError: (2013, 'Lost connection to MySQL server during query')
    time.sleep(15)

    with manager.get_connection() as conn:
        with conn.cursor() as cursor:
            sql = "SELECT variable_value AS version FROM global_variables WHERE variable_name='mysql-server_version'"
            cursor.execute(sql)
            result = cursor.fetchone()

            version = result['version']

    assert version == "5.5.30"


def test__can_register_mysql_backend(proxysql_container):
    assert proxysql_container.status == 'running'

    backend = ProxySQLMySQLBackend()
    backend.hostgroup_id = 10
    backend.hostname = 'test-host'
    backend.port = 10000

    container_ip = '127.0.0.1'

    admin_port_bindings = proxysql_container.attrs['NetworkSettings']['Ports']['%s/tcp' % PROXYSQL_ADMIN_PORT]
    admin_port = int(admin_port_bindings.pop()['HostPort'])
    assert admin_port

    manager = ProxySQLManager(host=container_ip, port=admin_port, user=PROXYSQL_ADMIN_USER,
                              password=PROXYSQL_ADMIN_PASSWORD)

    # Allow ProxySQL to startup completely. The problem is that ProxySQL starts listening to the admin port
    # before it has initialized completely which causes the test to fail with the exception:
    # OperationalError: (2013, 'Lost connection to MySQL server during query')
    time.sleep(15)

    assert manager.register_mysql_backend(backend.hostgroup_id, backend.hostname, backend.port)

    with manager.get_connection() as conn:
        assert manager.is_mysql_backend_registered(backend, conn)
