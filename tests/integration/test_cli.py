from click.testing import CliRunner

import proxysql_tools
from proxysql_tools.cli import main
from proxysql_tools.galera.galera_node import GaleraNode
from proxysql_tools.proxysql.proxysql import ProxySQLMySQLBackend, BackendStatus
from tests.integration.library import proxysql_tools_config, \
    wait_for_cluster_nodes_to_become_healthy, proxysql_tools_config_2, \
    shutdown_container
import pymysql
from pymysql.cursors import DictCursor


def test__main_command_version_can_be_fetched():
    runner = CliRunner()
    result = runner.invoke(main, ['--version'])
    assert result.output == proxysql_tools.__version__ + '\n'
    assert result.exit_code == 0


def test__ping_command_can_be_executed(proxysql_instance, tmpdir):
    config = proxysql_tools_config(proxysql_instance, '127.0.0.1', '3306',
                                   'user', 'pass', 10, 11, 'monitor',
                                   'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)
        proxysql_tools.LOG.debug('proxysql-tools config: \n%s', config)
    runner = CliRunner()
    result = runner.invoke(main, ['--config', config_file, 'ping'])
    assert result.exit_code == 0


def test__galera_register_command_set_nodes_online(percona_xtradb_cluster_three_node,
                                                   proxysql_instance,
                                                   tmpdir):
    wait_for_cluster_nodes_to_become_healthy(percona_xtradb_cluster_three_node)
    hostgroup_writer = 10
    hostgroup_reader = 11

    rw_map = {
        0: hostgroup_writer,
        1: hostgroup_reader,
        2: hostgroup_reader
    }
    for i in xrange(3):
        backend = ProxySQLMySQLBackend(
            hostname=percona_xtradb_cluster_three_node[i]['ip'],
            port=percona_xtradb_cluster_three_node[0]['mysql_port'],
            hostgroup_id=rw_map[i]
        )
        proxysql_instance.register_backend(backend)

    blacklist = '{}:3306'.format(percona_xtradb_cluster_three_node[2]['ip'])
    nodes = [
        percona_xtradb_cluster_three_node[0]['ip'] + ':3306',
        percona_xtradb_cluster_three_node[1]['ip'] + ':3306',
        percona_xtradb_cluster_three_node[2]['ip'] + ':3306'
    ]
    config = proxysql_tools_config_2(proxysql_instance,
                                     nodes,
                                     'root', 'r00t', hostgroup_writer,
                                     hostgroup_reader,
                                     blacklist, 'monitor',
                                     'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)
        proxysql_tools.LOG.debug('proxysql-tools config: \n%s', config)
    runner = CliRunner()
    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'register']
                           )
    assert result.exit_code == 0

    connection = pymysql.connect(
        host=proxysql_instance.host,
        port=proxysql_instance.port,
        user=proxysql_instance.user,
        passwd=proxysql_instance.password,
        connect_timeout=20,
        cursorclass=DictCursor)
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT `hostgroup_id`, `hostname`, '
                           '`port`, `status`, `weight`, `compression`, '
                           '`max_connections`, `max_replication_lag`, '
                           '`use_ssl`, `max_latency_ms`, `comment`'
                           ' FROM `mysql_servers`'
                           ' WHERE hostgroup_id = %s', hostgroup_writer)
            for row in cursor.fetchall():
                backend = ProxySQLMySQLBackend(row['hostname'],
                                               hostgroup_id=row['hostgroup_id'],
                                               port=row['port'],
                                               status=row['status'],
                                               weight=row['weight'],
                                               compression=row['compression'],
                                               max_connections=
                                               row['max_connections'],
                                               max_replication_lag=
                                               row['max_replication_lag'],
                                               use_ssl=row['use_ssl'],
                                               max_latency_ms=
                                               row['max_latency_ms'],
                                               comment=row['comment'])
                assert backend.status == BackendStatus.online

            cursor.execute('SELECT `hostgroup_id`, `hostname`, '
                           '`port`, `status`, `weight`, `compression`, '
                           '`max_connections`, `max_replication_lag`, '
                           '`use_ssl`, `max_latency_ms`, `comment`'
                           ' FROM `mysql_servers`'
                           ' WHERE hostgroup_id = %s', hostgroup_reader)
            for row in cursor.fetchall():
                backend = ProxySQLMySQLBackend(row['hostname'],
                                               hostgroup_id=row['hostgroup_id'],
                                               port=row['port'],
                                               status=row['status'],
                                               weight=row['weight'],
                                               compression=row['compression'],
                                               max_connections=
                                               row['max_connections'],
                                               max_replication_lag=
                                               row['max_replication_lag'],
                                               use_ssl=row['use_ssl'],
                                               max_latency_ms=
                                               row['max_latency_ms'],
                                               comment=row['comment'])
                assert backend.status == BackendStatus.online

    finally:
        connection.close()


def test__galera_register_shutdowned_reader_is_degistered(percona_xtradb_cluster_three_node,
                                                          proxysql_instance,
                                                          tmpdir):
    wait_for_cluster_nodes_to_become_healthy(percona_xtradb_cluster_three_node)
    hostgroup_writer = 10
    hostgroup_reader = 11

    backend = ProxySQLMySQLBackend(
        hostname=percona_xtradb_cluster_three_node[0]['ip'],
        port=percona_xtradb_cluster_three_node[0]['mysql_port'],
        hostgroup_id=hostgroup_writer
    )
    proxysql_instance.register_backend(backend)

    backend_unreg = ProxySQLMySQLBackend(
        hostname=percona_xtradb_cluster_three_node[1]['ip'],
        port=percona_xtradb_cluster_three_node[1]['mysql_port'],
        hostgroup_id=hostgroup_reader
    )
    proxysql_instance.register_backend(backend_unreg)

    backend = ProxySQLMySQLBackend(
        hostname=percona_xtradb_cluster_three_node[2]['ip'],
        port=percona_xtradb_cluster_three_node[2]['mysql_port'],
        hostgroup_id=hostgroup_reader
    )
    proxysql_instance.register_backend(backend)

    blacklist = '{}:3306'.format(percona_xtradb_cluster_three_node[2]['ip'])
    nodes = [
        percona_xtradb_cluster_three_node[0]['ip'] + ':3306',
        percona_xtradb_cluster_three_node[1]['ip'] + ':3306',
        percona_xtradb_cluster_three_node[2]['ip'] + ':3306'
    ]

    shutdown_container(percona_xtradb_cluster_three_node[1]['id'])
    config = proxysql_tools_config_2(proxysql_instance,
                                     nodes,
                                     'root', 'r00t', hostgroup_writer,
                                     hostgroup_reader,
                                     blacklist, 'monitor',
                                     'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)
        proxysql_tools.LOG.debug('proxysql-tools config: \n%s', config)
    runner = CliRunner()
    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'register']
                           )
    assert result.exit_code == 0

    connection = pymysql.connect(
        host=proxysql_instance.host,
        port=proxysql_instance.port,
        user=proxysql_instance.user,
        passwd=proxysql_instance.password,
        connect_timeout=20,
        cursorclass=DictCursor)
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT `hostgroup_id`, `hostname`, '
                           '`port`'
                           ' FROM `mysql_servers`'
                           ' WHERE hostgroup_id = %s '
                           ' AND `hostname` = %s '
                           ' AND `port` = %s',
                           (
                               backend_unreg.hostgroup_id,
                               backend_unreg.hostname,
                               backend_unreg.port
                           ))
            assert cursor.fetchall() == ()

    finally:
        connection.close()


def test__galera_register_shutdowned_writer_is_deregistered(percona_xtradb_cluster_three_node,
                                                            proxysql_instance,
                                                            tmpdir):
    wait_for_cluster_nodes_to_become_healthy(percona_xtradb_cluster_three_node)
    hostgroup_writer = 10
    hostgroup_reader = 11

    backend_unreg = ProxySQLMySQLBackend(
        hostname=percona_xtradb_cluster_three_node[0]['ip'],
        port=percona_xtradb_cluster_three_node[0]['mysql_port'],
        hostgroup_id=hostgroup_writer
    )
    proxysql_instance.register_backend(backend_unreg)

    backend = ProxySQLMySQLBackend(
        hostname=percona_xtradb_cluster_three_node[1]['ip'],
        port=percona_xtradb_cluster_three_node[1]['mysql_port'],
        hostgroup_id=hostgroup_reader
    )
    proxysql_instance.register_backend(backend)

    backend = ProxySQLMySQLBackend(
        hostname=percona_xtradb_cluster_three_node[2]['ip'],
        port=percona_xtradb_cluster_three_node[2]['mysql_port'],
        hostgroup_id=hostgroup_reader
    )
    proxysql_instance.register_backend(backend)

    blacklist = '{}:3306'.format(percona_xtradb_cluster_three_node[2]['ip'])
    nodes = [
        percona_xtradb_cluster_three_node[0]['ip'] + ':3306',
        percona_xtradb_cluster_three_node[1]['ip'] + ':3306',
        percona_xtradb_cluster_three_node[2]['ip'] + ':3306'
    ]

    shutdown_container(percona_xtradb_cluster_three_node[0]['id'])
    config = proxysql_tools_config_2(proxysql_instance,
                                     nodes,
                                     'root', 'r00t', hostgroup_writer,
                                     hostgroup_reader,
                                     blacklist, 'monitor',
                                     'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)
        proxysql_tools.LOG.debug('proxysql-tools config: \n%s', config)
    runner = CliRunner()
    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'register']
                           )
    assert result.exit_code == 0

    connection = pymysql.connect(
        host=proxysql_instance.host,
        port=proxysql_instance.port,
        user=proxysql_instance.user,
        passwd=proxysql_instance.password,
        connect_timeout=20,
        cursorclass=DictCursor
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT `hostgroup_id`, `hostname`, '
                           '`port`'
                           ' FROM `mysql_servers`'
                           ' WHERE hostgroup_id = %s '
                           ' AND `hostname` = %s '
                           ' AND `port` = %s',
                           (
                               backend_unreg.hostgroup_id,
                               backend_unreg.hostname,
                               backend_unreg.port
                           )
                           )
            assert cursor.fetchall() == ()

            cursor.execute('SELECT `hostgroup_id`, `hostname`, '
                           '`port`'
                           ' FROM `mysql_servers`'
                           ' WHERE hostgroup_id = %s ',
                           (
                               backend_unreg.hostgroup_id,
                           )
                           )
            assert cursor.fetchall() != ()

    finally:
        connection.close()


def test__galera_register_sync_desync_state(percona_xtradb_cluster_three_node,
                                                   proxysql_instance,
                                                   tmpdir):
    wait_for_cluster_nodes_to_become_healthy(percona_xtradb_cluster_three_node)
    hostgroup_writer = 10
    hostgroup_reader = 11

    rw_map = {
        0: hostgroup_writer,
        1: hostgroup_reader,
        2: hostgroup_reader
    }
    for i in xrange(3):
        backend = ProxySQLMySQLBackend(
            hostname=percona_xtradb_cluster_three_node[i]['ip'],
            port=percona_xtradb_cluster_three_node[0]['mysql_port'],
            hostgroup_id=rw_map[i]
        )
        proxysql_instance.register_backend(backend)


    desync_node = GaleraNode(
        host=percona_xtradb_cluster_three_node[1]['ip'],
        port=percona_xtradb_cluster_three_node[1]['mysql_port'],
        user='root',password='r00t'
    )
    desync_node.execute('set global wsrep_desync=ON;')

    blacklist = '{}:3306'.format(percona_xtradb_cluster_three_node[2]['ip'])
    nodes = [
        percona_xtradb_cluster_three_node[0]['ip'] + ':3306',
        percona_xtradb_cluster_three_node[1]['ip'] + ':3306',
        percona_xtradb_cluster_three_node[2]['ip'] + ':3306'
    ]
    config = proxysql_tools_config_2(proxysql_instance,
                                     nodes,
                                     'root', 'r00t', hostgroup_writer,
                                     hostgroup_reader,
                                     blacklist, 'monitor',
                                     'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)
        proxysql_tools.LOG.debug('proxysql-tools config: \n%s', config)
    runner = CliRunner()
    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'register'])
    assert result.exit_code == 0

    connection = pymysql.connect(
        host=proxysql_instance.host,
        port=proxysql_instance.port,
        user=proxysql_instance.user,
        passwd=proxysql_instance.password,
        connect_timeout=20,
        cursorclass=DictCursor)
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT `hostgroup_id`, `hostname`, '
                           '`port`, `status`, `weight`, `compression`, '
                           '`max_connections`, `max_replication_lag`, '
                           '`use_ssl`, `max_latency_ms`, `comment`'
                           ' FROM `mysql_servers`'
                           ' WHERE hostgroup_id = %s'
                           ' AND hostname = %s',
                           (
                               hostgroup_reader,
                               percona_xtradb_cluster_three_node[1]['ip']
                           )
                           )
            row = cursor.fetchall()[0]
            assert row['status'] == BackendStatus.offline_soft
    finally:
        connection.close()

    desync_node.execute('set global wsrep_desync=OFF;')
    runner = CliRunner()
    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'register'])
    assert result.exit_code == 0
    connection = pymysql.connect(
        host=proxysql_instance.host,
        port=proxysql_instance.port,
        user=proxysql_instance.user,
        passwd=proxysql_instance.password,
        connect_timeout=20,
        cursorclass=DictCursor)
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT `hostgroup_id`, `hostname`, '
                           '`port`, `status`, `weight`, `compression`, '
                           '`max_connections`, `max_replication_lag`, '
                           '`use_ssl`, `max_latency_ms`, `comment`'
                           ' FROM `mysql_servers`'
                           ' WHERE hostgroup_id = %s'
                           ' AND hostname = %s',
                           (
                               hostgroup_reader,
                               percona_xtradb_cluster_three_node[1]['ip']
                           )
                           )
            row = cursor.fetchall()[0]
            assert row['status'] == BackendStatus.online
    finally:
        connection.close()
