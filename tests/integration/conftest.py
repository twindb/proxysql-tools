import os
import pprint
import pytest

from docker.types import IPAMConfig, IPAMPool

from proxysql_tools import LOG, setup_logging
from proxysql_tools.galera.galera_node import GaleraNode
from proxysql_tools.proxysql.proxysql import ProxySQL
from tests.integration.library import (
    docker_client,
    docker_pull_image,
    eventually,
    create_percona_xtradb_cluster
)


CONTAINERS_FOR_TESTING_LABEL = 'pytest_docker'
DEBIAN_IMAGE = 'debian:8'
PROXYSQL_IMAGE = 'twindb/proxysql:latest'
PXC_IMAGE = 'twindb/percona-xtradb-cluster:latest'

PROXYSQL_ADMIN_PORT = 6032
PROXYSQL_CLIENT_PORT = 6033
PROXYSQL_ADMIN_USER = 'admin'
PROXYSQL_ADMIN_PASSWORD = 'admin'

PXC_MYSQL_PORT = 3306
PXC_ROOT_PASSWORD = 'r00t'

setup_logging(LOG, debug=True)


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
            report.longrepr.addsection('docker logs',
                                       os.linesep.join(log_lines))


@pytest.yield_fixture
def debian_container():
    client = docker_client()
    api = client.api

    # Pull the image locally
    docker_pull_image(DEBIAN_IMAGE)

    container = api.create_container(
        image=DEBIAN_IMAGE, labels=[CONTAINERS_FOR_TESTING_LABEL],
        command='/bin/sleep 36000')
    LOG.debug('Starting container %s', container['Id'])
    api.start(container['Id'])

    container_info = client.containers.get(container['Id'])

    yield container_info

    api.remove_container(container=container['Id'], force=True)


@pytest.yield_fixture(scope='session')
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
             admin_port=PROXYSQL_ADMIN_PORT,
             client_port=PROXYSQL_CLIENT_PORT)


@pytest.yield_fixture
def proxysql_container(proxysql_config_contents, tmpdir, container_network):
    client = docker_client()
    api = client.api

    # Pull the container image locally first
    docker_pull_image(PROXYSQL_IMAGE)

    # Setup the ProxySQL config
    config = tmpdir.join('proxysql.cnf')
    config.write(proxysql_config_contents)
    LOG.debug('ProxySQL Config:\n %s', proxysql_config_contents)

    # The ports that the ProxySQL container will be listening on inside the
    # container
    container_ports = [PROXYSQL_ADMIN_PORT, PROXYSQL_CLIENT_PORT]
    container_info = {
        'name': 'proxysql01',
        'ip': '172.25.3.100',
    }

    host_config = api.create_host_config(
        binds=["{}:/etc/proxysql.cnf".format(str(config))],
        port_bindings={}
    )

    networking_config = api.create_networking_config({
        container_network: api.create_endpoint_config(
            ipv4_address=container_info['ip']
        )
    })

    container = api.create_container(image=PROXYSQL_IMAGE,
                                     name='proxysql01',
                                     labels=[CONTAINERS_FOR_TESTING_LABEL],
                                     ports=container_ports,
                                     host_config=host_config,
                                     networking_config=networking_config)
    LOG.debug('Starting container %s', container['Id'])
    api.start(container['Id'])
    # 1/0

    yield container_info

    api.remove_container(container=container['Id'], force=True)


@pytest.fixture
def proxysql_instance(proxysql_container):
    LOG.debug("Container %r", proxysql_container)
    connection = ProxySQL(host=proxysql_container['ip'],
                          port=PROXYSQL_ADMIN_PORT,
                          user=PROXYSQL_ADMIN_USER,
                          password=PROXYSQL_ADMIN_PASSWORD)

    def check_started():
        LOG.debug('Checking if proxysql is up')
        ret = connection.ping()
        LOG.debug(ret)
        return ret

    # Allow ProxySQL to startup completely. The problem is that ProxySQL starts
    # listening to the admin port before it has initialized completely which
    # causes the test to fail with the exception:
    # OperationalError: (2013, 'Lost connection to MySQL server during query')
    eventually(check_started, retries=15, sleep_time=4)

    return connection


@pytest.yield_fixture
def percona_xtradb_cluster_three_node(container_network):
    client = docker_client()
    api = client.api

    # The ports that need to be exposed from the PXC container to host
    container_ports = [PXC_MYSQL_PORT]
    container_info = [
        {
            'id': None,
            'name': 'pxc-node01',
            'ip': '172.25.3.1',
            'mysql_port': PXC_MYSQL_PORT,
            'root_password': PXC_ROOT_PASSWORD
        },
        {
            'id': None,
            'name': 'pxc-node02',
            'ip': '172.25.3.2',
            'mysql_port': PXC_MYSQL_PORT,
            'root_password': PXC_ROOT_PASSWORD
        },
        {
            'id': None,
            'name': 'pxc-node03',
            'ip': '172.25.3.3',
            'mysql_port': PXC_MYSQL_PORT,
            'root_password': PXC_ROOT_PASSWORD
        }
    ]
    cluster_name = 'test_cluster_3_node'

    # Create the cluster
    container_info = create_percona_xtradb_cluster(
        PXC_IMAGE, [CONTAINERS_FOR_TESTING_LABEL], container_info,
        container_ports, container_network, cluster_name
    )

    yield container_info

    # Cleanup the containers now
    for container in container_info:
        api.remove_container(container=container['id'], force=True)


@pytest.yield_fixture
def percona_xtradb_cluster_one_node(container_network):
    client = docker_client()
    api = client.api

    # The ports that need to be exposed from the PXC container to host
    container_ports = [PXC_MYSQL_PORT]
    container_info = [
        {
            'id': None,
            'name': 'pxc-one-node-node01',
            'ip': '172.25.3.10',
            'mysql_port': PXC_MYSQL_PORT,
            'root_password': PXC_ROOT_PASSWORD
        }
    ]
    cluster_name = 'test_cluster_1_node'

    # Create the cluster
    container_info = create_percona_xtradb_cluster(
        PXC_IMAGE, [CONTAINERS_FOR_TESTING_LABEL], container_info,
        container_ports, container_network, cluster_name
    )

    yield container_info

    # Cleanup the containers now
    for container in container_info:
        api.remove_container(container=container['id'], force=True)


@pytest.fixture
def percona_xtradb_cluster_node(percona_xtradb_cluster_one_node):
    node = GaleraNode(host=percona_xtradb_cluster_one_node[0]['ip'],
                      user='root',
                      password=PXC_ROOT_PASSWORD
    )

    def check_started():
        node.execute('SELECT 1')
        return True

    # Allow the cluster node to startup completely.
    eventually(check_started, retries=15, sleep_time=4)

    return node
