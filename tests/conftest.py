import os
import pprint
import pytest
import time

from docker.types import IPAMConfig, IPAMPool
from proxysql_tools.entities.proxysql import (
    ProxySQLMySQLBackend, ProxySQLMySQLUser
)
from proxysql_tools.managers.proxysql_manager import ProxySQLManager
from tests.library import get_unused_port, docker_client, docker_pull_image


CONTAINERS_FOR_TESTING_LABEL = 'pytest_docker'
DEBIAN_IMAGE = 'debian:8'
PROXYSQL_IMAGE = 'twindb/proxysql:latest'
PXC_IMAGE = 'percona/percona-xtradb-cluster:5.7.16'

PROXYSQL_ADMIN_PORT = 6032
PROXYSQL_CLIENT_PORT = 6033
PROXYSQL_ADMIN_USER = 'admin'
PROXYSQL_ADMIN_PASSWORD = 'admin'

PXC_MYSQL_PORT = 3306


def pytest_runtest_logreport(report):
    """Process a test setup/call/teardown report relating to the respective
        phase of executing a test."""
    if report.failed:
        client = docker_client()
        api = client.api

        test_containers = api.containers(
            all=True, filters={"label": CONTAINERS_FOR_TESTING_LABEL})

        for container in test_containers:
            log_lines = [
                ("docker inspect {!r}:".format(container['Id'])),
                (pprint.pformat(api.inspect_container(container['Id']))),
                ("docker logs {!r}:".format(container['Id'])),
                (api.logs(container['Id'])),
            ]
            report.longrepr.addsection('docker logs', os.linesep.join(log_lines))


@pytest.yield_fixture
def debian_container():
    client = docker_client()
    api = client.api

    # Pull the image locally
    docker_pull_image(DEBIAN_IMAGE)

    container = api.create_container(
        image=DEBIAN_IMAGE, labels=[CONTAINERS_FOR_TESTING_LABEL],
        command='/bin/sleep 36000')
    api.start(container['Id'])

    container_info = client.containers.get(container['Id'])

    yield container_info

    api.remove_container(container=container['Id'], force=True)


@pytest.yield_fixture
def container_network():
    client = docker_client()
    api = client.api

    network_name = 'test_network'

    ipam_pool = IPAMPool(subnet="172.25.0.0/16")
    ipam_config = IPAMConfig(pool_configs=[ipam_pool])

    network = api.create_network(name=network_name, driver="bridge",
                                 ipam=ipam_config)

    yield network_name

    api.remove_network(net_id=network['Id'])


@pytest.fixture
def proxysql_config_contents():
    return """
datadir="/var/lib/proxysql"

admin_variables=
{{
        admin_credentials="{admin_user}:{admin_password}"
        mysql_ifaces="0.0.0.0:{admin_port};/var/lib/proxysql/proxysql_admin.sock"
        refresh_interval=2000
        debug=false
}}

mysql_variables=
{{
        default_query_delay=0
        default_query_timeout=36000000
        have_compress=true
        poll_timeout=2000
        stacksize=1048576
        server_version="5.5.30"
        threads=8
        max_connections=2048
        interfaces="0.0.0.0:{client_port};/var/lib/proxysql/proxysql.sock"
        default_schema="information_schema"
        connect_timeout_server=1000
        connect_timeout_server_max=3000
        connect_retries_on_failure=3
        monitor_enabled=true
        monitor_history=600000
        monitor_connect_interval=60000
        monitor_ping_interval=1000
        monitor_ping_timeout=100
        ping_interval_server=10000
        ping_timeout_server=500
        commands_stats=true
        sessions_sort=true
}}""".format(admin_user=PROXYSQL_ADMIN_USER,
             admin_password=PROXYSQL_ADMIN_PASSWORD,
             admin_port=PROXYSQL_ADMIN_PORT, client_port=PROXYSQL_CLIENT_PORT)


@pytest.yield_fixture
def proxysql_container(proxysql_config_contents, tmpdir):
    client = docker_client()
    api = client.api

    # Setup the ProxySQL config
    config = tmpdir.join('proxysql.cnf')
    config.write(proxysql_config_contents)

    # The ports that the ProxySQL container will be listening on inside the
    # container
    container_ports = [PROXYSQL_ADMIN_PORT, PROXYSQL_CLIENT_PORT]

    host_config = api.create_host_config(binds=[
        "{}:/etc/proxysql.cnf".format(str(config))
    ], port_bindings={
        PROXYSQL_ADMIN_PORT: get_unused_port(),
        PROXYSQL_CLIENT_PORT: get_unused_port()
    })

    # Pull the container image locally first
    docker_pull_image(PROXYSQL_IMAGE)

    container = api.create_container(image=PROXYSQL_IMAGE,
                                     labels=[CONTAINERS_FOR_TESTING_LABEL],
                                     ports=container_ports,
                                     host_config=host_config)
    api.start(container['Id'])

    container_info = client.containers.get(container['Id'])

    yield container_info

    api.remove_container(container=container['Id'], force=True)


@pytest.yield_fixture
def percona_xtradb_cluster(container_network):
    client = docker_client()
    api = client.api

    # The ports that need to be exposed from the PXC container to host
    container_ports = [PXC_MYSQL_PORT]
    container_info = {
        'pxc01': {
            'ip': '172.25.3.1',
            'mysql_port': None,
            'id': None
        },
        'pxc02': {
            'ip': '172.25.3.2',
            'mysql_port': None,
            'id': None
        },
        'pxc03': {
            'ip': '172.25.3.3',
            'mysql_port': None,
            'id': None
        }
    }
    cluster_name = 'test_cluster'

    # Pull the container image locally first
    docker_pull_image(PXC_IMAGE)

    bootstrapped = False
    cluster_join = ''
    for node_name, node_details in container_info.iteritems():
        available_port = get_unused_port()
        container_info[node_name]['mysql_port'] = available_port

        host_config = api.create_host_config(port_bindings={
            PXC_MYSQL_PORT: available_port
        })

        networking_config = api.create_networking_config({
            container_network: api.create_endpoint_config(
                ipv4_address=node_details['ip']
            )
        })

        environment_vars = {
            'MYSQL_ALLOW_EMPTY_PASSWORD': 1,
            'CLUSTER_JOIN': cluster_join,
            'CLUSTER_NAME': cluster_name,
            'XTRABACKUP_PASSWORD': 'xtrabackup'
        }

        container = api.create_container(
            image=PXC_IMAGE, name=node_name,
            labels=[CONTAINERS_FOR_TESTING_LABEL], ports=container_ports,
            host_config=host_config, networking_config=networking_config,
            environment=environment_vars,
            command='--wsrep_node_address=%s' % node_details['ip'])
        api.start(container['Id'])
        container_info[node_name]['id'] = container['Id']

        if not bootstrapped:
            cluster_join = node_details['ip']
            bootstrapped = True

        # We add a bit of delay to allow the node to startup before adding a
        # new node to the cluster
        time.sleep(5)

    yield container_info

    # Cleanup the containers now
    for container in container_info.values():
        api.remove_container(container=container['id'], force=True)


@pytest.fixture
def proxysql_manager(proxysql_container):
    assert proxysql_container.status == 'running'

    container_ip = '127.0.0.1'

    admin_port_bindings = proxysql_container.attrs['NetworkSettings']['Ports']['%s/tcp' % PROXYSQL_ADMIN_PORT]
    admin_port = int(admin_port_bindings.pop()['HostPort'])
    assert admin_port

    manager = ProxySQLManager(host=container_ip, port=admin_port,
                              user=PROXYSQL_ADMIN_USER,
                              password=PROXYSQL_ADMIN_PASSWORD)

    # Allow ProxySQL to startup completely. The problem is that ProxySQL starts
    # listening to the admin port before it has initialized completely which
    # causes the test to fail with the exception:
    # OperationalError: (2013, 'Lost connection to MySQL server during query')
    time.sleep(15)

    return manager


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