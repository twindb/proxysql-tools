from click.testing import CliRunner

import proxysql_tools
from proxysql_tools.cli import main
from proxysql_tools.galera.galera_node import GaleraNode
from proxysql_tools.proxysql.proxysql import ProxySQLMySQLBackend, BackendStatus, ProxySQLMySQLUser
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


def test__galera_register_writer_node_is_reader_when_readers_list_is_empty(percona_xtradb_cluster_three_node,
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

    desync_node = GaleraNode(
        host=percona_xtradb_cluster_three_node[2]['ip'],
        port=percona_xtradb_cluster_three_node[2]['mysql_port'],
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
            cursor.execute('SELECT `hostgroup_id`, `hostname`, '
                           '`port`, `status`, `weight`, `compression`, '
                           '`max_connections`, `max_replication_lag`, '
                           '`use_ssl`, `max_latency_ms`, `comment`'
                           ' FROM `mysql_servers`'
                           ' WHERE hostgroup_id = %s'
                           ' AND hostname = %s',
                           (
                               hostgroup_reader,
                               percona_xtradb_cluster_three_node[2]['ip']
                           )
                           )
            row = cursor.fetchall()[0]
            assert row['status'] == BackendStatus.offline_soft

            cursor.execute('SELECT `hostgroup_id`, `hostname`, '
                           '`port`, `status`, `weight`, `compression`, '
                           '`max_connections`, `max_replication_lag`, '
                           '`use_ssl`, `max_latency_ms`, `comment`'
                           ' FROM `mysql_servers`'
                           ' WHERE hostgroup_id = %s'
                           ' AND hostname = %s',
                           (
                               hostgroup_reader,
                               percona_xtradb_cluster_three_node[0]['ip']
                           )
                           )
            assert cursor.fetchall() != ()

    finally:
        connection.close()


def test__galera_server_status(percona_xtradb_cluster_three_node,
                        proxysql_instance, tmpdir):
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

    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'server', 'status']
                           )
    assert result.exit_code == 0


def test__galera_server_set_desync(percona_xtradb_cluster_three_node,
                        proxysql_instance, tmpdir):
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

    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'server', 'set_desync',
                            percona_xtradb_cluster_three_node[1]['ip'], '3306']
                           )
    assert result.exit_code == 0

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


def test__galera_server_set_sync(percona_xtradb_cluster_three_node,
                                   proxysql_instance, tmpdir):
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

    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'server', 'set_sync',
                            percona_xtradb_cluster_three_node[1]['ip'], '3306']
                           )
    assert result.exit_code == 0

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


def test__galera_user_create_using_options(proxysql_instance, tmpdir):
    config = proxysql_tools_config(proxysql_instance, '127.0.0.1', '3306',
                                   'user', 'pass', 10, 11, 'monitor',
                                   'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)
        proxysql_tools.LOG.debug('proxysql-tools config: \n%s', config)
    runner = CliRunner()
    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'user', 'create', 'foo']
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
            cursor.execute("SELECT * FROM mysql_users WHERE username = '{username}'"
                              .format(username='foo'))
            row = cursor.fetchall()[0]
            assert row
            user = ProxySQLMySQLUser(username=row['username'],
                                     password=row['password'],
                                     active=row['active'],
                                     use_ssl=row['use_ssl'],
                                     default_hostgroup=row['default_hostgroup'],
                                     default_schema=row['default_schema'],
                                     schema_locked=row['schema_locked'],
                                     transaction_persistent=row['transaction_persistent'],
                                     fast_forward=row['fast_forward'],
                                     backend=row['backend'],
                                     frontend=row['frontend'],
                                     max_connections=row['max_connections'])
            assert user.active
            assert not user.use_ssl
            assert user.default_hostgroup == 0
            assert user.default_schema == 'information_schema'
            assert not user.schema_locked
            assert not user.transaction_persistent
            assert not user.fast_forward
            assert user.backend
            assert user.frontend
            assert user.max_connections == 10000

    finally:
        connection.close()


def test__galera_user_set_password_if_user_is_exist(percona_xtradb_cluster_three_node,
                                                    proxysql_instance, tmpdir):
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

    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'user', 'create', 'foo']
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
            cursor.execute("SELECT * FROM mysql_users WHERE username = '{username}'"
                           .format(username='foo'))
            assert cursor.fetchall() != ()

    finally:
        connection.close()

    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'user', 'set_password', 'foo'],
                           input='test\ntest\n'
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
            cursor.execute("SELECT * FROM mysql_users WHERE username = '{username}'"
                           .format(username='foo'))

            row = cursor.fetchall()[0]
            user = ProxySQLMySQLUser(username=row['username'],
                                     password=row['password'],
                                     active=row['active'],
                                     use_ssl=row['use_ssl'],
                                     default_hostgroup=row['default_hostgroup'],
                                     default_schema=row['default_schema'],
                                     schema_locked=row['schema_locked'],
                                     transaction_persistent=row['transaction_persistent'],
                                     fast_forward=row['fast_forward'],
                                     backend=row['backend'],
                                     frontend=row['frontend'],
                                     max_connections=row['max_connections'])
            assert user.password
            assert user.active
            assert not user.use_ssl
            assert user.default_hostgroup == 0
            assert user.default_schema == 'information_schema'
            assert not user.schema_locked
            assert not user.transaction_persistent
            assert not user.fast_forward
            assert user.backend
            assert user.frontend
            assert user.max_connections == 10000
    finally:
        connection.close()


def test__galera_user_set_password_if_user_is_not_exist(proxysql_instance, tmpdir):
    config = proxysql_tools_config(proxysql_instance, '127.0.0.1', '3306',
                                   'user', 'pass', 10, 11, 'monitor',
                                   'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)
        proxysql_tools.LOG.debug('proxysql-tools config: \n%s', config)
    runner = CliRunner()

    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'user', 'set_password', 'foo'],
                           input='test\ntest\n'
                           )

    assert result.exit_code == 1


def test__galera_user_list_can_be_executed(proxysql_instance, tmpdir):
    config = proxysql_tools_config(proxysql_instance, '127.0.0.1', '3306',
                                   'user', 'pass', 10, 11, 'monitor',
                                   'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)
        proxysql_tools.LOG.debug('proxysql-tools config: \n%s', config)
    runner = CliRunner()
    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'user', 'create', 'foo']
                           )
    assert result.exit_code == 0
    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'user', 'list']
                           )
    assert result.exit_code == 0


def test__galera_user_delete(proxysql_instance, tmpdir):
    config = proxysql_tools_config(proxysql_instance, '127.0.0.1', '3306',
                                   'user', 'pass', 10, 11, 'monitor',
                                   'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)
        proxysql_tools.LOG.debug('proxysql-tools config: \n%s', config)
    runner = CliRunner()
    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'user', 'create', 'foo']
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
            cursor.execute("SELECT * FROM mysql_users WHERE username = '{username}'"
                           .format(username='foo'))
            assert cursor.fetchall() != ()

    finally:
        connection.close()
    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'user', 'delete', 'foo']
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
            cursor.execute("SELECT * FROM mysql_users WHERE username = '{username}'"
                           .format(username='foo'))
            assert cursor.fetchall() == ()

    finally:
        connection.close()


def test__galera_user_modify_user_if_user_is_exist(proxysql_instance, tmpdir):

    config = proxysql_tools_config(proxysql_instance, '127.0.0.1', '3306',
                                   'user', 'pass', 10, 11, 'monitor',
                                   'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)
        proxysql_tools.LOG.debug('proxysql-tools config: \n%s', config)
    runner = CliRunner()

    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'user', 'create', 'foo']
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
            cursor.execute("SELECT * FROM mysql_users WHERE username = '{username}'"
                           .format(username='foo'))
            row = cursor.fetchall()[0]
            user = ProxySQLMySQLUser(username=row['username'],
                                     password=row['password'],
                                     active=row['active'],
                                     use_ssl=row['use_ssl'],
                                     default_hostgroup=row['default_hostgroup'],
                                     default_schema=row['default_schema'],
                                     schema_locked=row['schema_locked'],
                                     transaction_persistent=row['transaction_persistent'],
                                     fast_forward=row['fast_forward'],
                                     backend=row['backend'],
                                     frontend=row['frontend'],
                                     max_connections=row['max_connections'])
            assert not user.password
            assert user.active
            assert not user.use_ssl
            assert user.default_hostgroup == 0
            assert user.default_schema == 'information_schema'
            assert not user.schema_locked
            assert not user.transaction_persistent
            assert not user.fast_forward
            assert user.backend
            assert user.frontend
            assert user.max_connections == 10000

    finally:
        connection.close()

    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'user', 'modify', 'foo',
                            '--no-active', '--use_ssl', '--default_hostgroup', '1',
                            '--default_schema', 'foo', '--schema_locked', '--transaction_persistent',
                            '--fast_forward', '--no-backend', '--no-frontend', '--max_connections',
                            '100']
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
            cursor.execute("SELECT * FROM mysql_users WHERE username = '{username}'"
                           .format(username='foo'))
            row = cursor.fetchall()[0]
            user = ProxySQLMySQLUser(username=row['username'],
                                     password=row['password'],
                                     active=row['active'],
                                     use_ssl=row['use_ssl'],
                                     default_hostgroup=row['default_hostgroup'],
                                     default_schema=row['default_schema'],
                                     schema_locked=row['schema_locked'],
                                     transaction_persistent=row['transaction_persistent'],
                                     fast_forward=row['fast_forward'],
                                     backend=row['backend'],
                                     frontend=row['frontend'],
                                     max_connections=row['max_connections'])
            assert not user.password
            assert not user.active
            assert user.use_ssl
            assert user.default_hostgroup == 1
            assert user.default_schema == 'foo'
            assert user.schema_locked
            assert user.transaction_persistent
            assert user.fast_forward
            assert not user.backend
            assert not user.frontend
            assert user.max_connections == 100

    finally:
        connection.close()


def test__galera_user_modify_user_if_user_is_not_exist(proxysql_instance, tmpdir):
    config = proxysql_tools_config(proxysql_instance, '127.0.0.1', '3306',
                                   'user', 'pass', 10, 11, 'monitor',
                                   'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)
        proxysql_tools.LOG.debug('proxysql-tools config: \n%s', config)
    runner = CliRunner()
    result = runner.invoke(main,
                           ['--config', config_file, 'galera', 'user', 'modify', 'foo',
                            '--no-active', '--use_ssl', '--default_hostgroup', '1',
                            '--default_schema', 'foo', '--schema_locked', '--transaction_persistent',
                            '--fast_forward', '--no-backend', '--no-frontend', '--max_connections',
                            '100']
                           )
    assert result.exit_code == 1




