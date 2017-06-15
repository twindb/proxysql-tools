import docker
import json
import socket
import time
import io
from ConfigParser import ConfigParser

from proxysql_tools import LOG
from proxysql_tools.galera.galera_node import GaleraNode


def docker_client():
    return docker.DockerClient(version="auto")


def docker_pull_image(image):
    """Pull the specified image using docker-py. This function will parse the
        result from docker-py and raise an exception if there is an error.

    :param str image: Name of the image to pull
    """
    client = docker_client()
    api = client.api

    response = api.pull(image)
    LOG.debug('Response: %s', response)


def get_unused_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    s.bind(('localhost', 0))
    _, port = s.getsockname()
    s.close()

    return port


def is_port_reachable(host, port):
    s = socket.socket()
    try:
        s.connect((host, port))
        return True
    except socket.error:
        return False


def eventually(func, *args, **kwargs):
    retries = kwargs.pop('retries', 90)
    sleep_time = kwargs.pop('sleep_time', 0.5)

    for i in xrange(retries):
        try:
            if func(*args, **kwargs):
                return
            else:
                LOG.info('Waiting for %s to return True', func)
                time.sleep(sleep_time)
        except Exception:
            time.sleep(sleep_time)
            continue

    raise EnvironmentError('Function %s never returned True' % func)


def shutdown_container(id):
    client = docker_client()
    api = client.api
    api.stop(id)

def create_percona_xtradb_cluster(container_image, container_labels,
                                  container_info, container_ports,
                                  network_name, cluster_name):
    client = docker_client()
    api = client.api

    # Pull the container image locally first
    docker_pull_image(container_image)

    bootstrapped = False
    cluster_join = ''
    for node in container_info:
        host_config = api.create_host_config(port_bindings={
            node['mysql_port']: get_unused_port()
        })

        networking_config = api.create_networking_config({
            network_name: api.create_endpoint_config(
                ipv4_address=node['ip']
            )
        })

        environment_vars = {
            'MYSQL_ROOT_PASSWORD': node['root_password'],
            'CLUSTER_JOIN': cluster_join,
            'CLUSTER_NAME': cluster_name,
            'XTRABACKUP_PASSWORD': 'xtrabackup'
        }

        container = api.create_container(
            image=container_image, name=node['name'],
            labels=container_labels, ports=container_ports,
            host_config=host_config, networking_config=networking_config,
            environment=environment_vars)
        api.start(container['Id'])

        node['id'] = container['Id']

        if not bootstrapped:
            cluster_join = node['ip']
            bootstrapped = True

        # We add a bit of delay to allow the node to startup before adding a
        # new node to the cluster
        time.sleep(5)

    return container_info


def wait_for_cluster_nodes_to_become_healthy(percona_xtradb_cluster_info):
    def check_started():
        for container_info in percona_xtradb_cluster_info:
            node = GaleraNode(host=container_info['ip'],
                              user='root',
                              password=container_info['root_password'])
            node.execute('SELECT 1')
        return True

    # Allow all the cluster nodes to startup.
    eventually(check_started, retries=20, sleep_time=4)


def proxysql_tools_config(proxysql_instance, cluster_host, cluster_port,
                          cluster_user, cluster_pass, hostgroup_writer,
                          hostgroup_reader, monitor_user, monitor_pass):
    config_contents = """
[proxysql]
host={proxy_host}
admin_port={proxy_port}
admin_username={proxy_user}
admin_password={proxy_pass}

monitor_username={monitor_user}
monitor_password={monitor_pass}

[galera]
cluster_host={cluster_host}:{cluster_port}
cluster_username={cluster_user}
cluster_password={cluster_pass}

load_balancing_mode=singlewriter

writer_hostgroup_id={writer_hostgroup}
reader_hostgroup_id={reader_hostgroup}
""".format(proxy_host=proxysql_instance.host, proxy_port=proxysql_instance.port,
           proxy_user=proxysql_instance.user,
           proxy_pass=proxysql_instance.password, monitor_user=monitor_user,
           monitor_pass=monitor_pass, cluster_host=cluster_host,
           cluster_port=cluster_port, cluster_user=cluster_user,
           cluster_pass=cluster_pass, writer_hostgroup=hostgroup_writer,
           reader_hostgroup=hostgroup_reader)

    config = ConfigParser()
    config.readfp(io.BytesIO(config_contents))
    return config

def proxysql_tools_config_2(proxysql_instance, cluster_nodes,
                             cluster_user, cluster_pass,
                             hostgroup_writer, hostgroup_reader, writer_blacklist,
                             monitor_user, monitor_pass):

    hosts = ','.join(cluster_nodes)
    config_contents = """
[proxysql]
host={proxy_host}
admin_port={proxy_port}
admin_username={proxy_user}
admin_password={proxy_pass}

monitor_username={monitor_user}
monitor_password={monitor_pass}

[galera]
cluster_host={hosts}
cluster_username={cluster_user}
cluster_password={cluster_pass}

load_balancing_mode=singlewriter
writer_blacklist={writer_blacklist}
writer_hostgroup_id={writer_hostgroup}
reader_hostgroup_id={reader_hostgroup}
""".format(proxy_host=proxysql_instance.host, proxy_port=proxysql_instance.port,
           proxy_user=proxysql_instance.user,
           proxy_pass=proxysql_instance.password, monitor_user=monitor_user,
           monitor_pass=monitor_pass, hosts=hosts,
           cluster_user=cluster_user,
           writer_blacklist=writer_blacklist,
           cluster_pass=cluster_pass, writer_hostgroup=hostgroup_writer,
           reader_hostgroup=hostgroup_reader)

    config = ConfigParser()
    config.readfp(io.BytesIO(config_contents))
    return config
