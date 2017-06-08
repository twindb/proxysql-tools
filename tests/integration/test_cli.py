from click.testing import CliRunner

from proxysql_tools.cli import main
from tests.integration.library import proxysql_tools_config
import pymysql
import time
from pymysql.cursors import DictCursor


def test__main_command_version_can_be_fetched():
    runner = CliRunner()
    result = runner.invoke(main, ['--version'])
    assert result.exit_code == 0


def test__ping_command_can_be_executed(proxysql_instance, tmpdir):
    config = proxysql_tools_config(proxysql_instance, '127.0.0.1', '3306',
                                   'user', 'pass', 10, 11, 'monitor',
                                   'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)
    runner = CliRunner()
    result = runner.invoke(main, ['--config', config_file, 'ping'])
    assert result.exit_code == 0


def test__galera_register_command_can_register_cluster_with_proxysql(
        percona_xtradb_cluster_node, proxysql_instance, tmpdir):
    hostgroup_writer = 10
    hostgroup_reader = 11

    connection = pymysql.connect(host=proxysql_instance.host, port=proxysql_instance.port,
                           user=proxysql_instance.user, passwd=proxysql_instance.password,
                           connect_timeout=20,
                           cursorclass=DictCursor)
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM mysql_servers '
                           ' WHERE hostgroup_id = %s ', hostgroup_writer)
            count = cursor.fetchone().values()[0]
            assert int(count) == 0
            cursor.execute('SELECT COUNT(*) FROM mysql_servers '
                           ' WHERE hostgroup_id = %s ', hostgroup_reader)
            count = cursor.fetchone().values()[0]
            assert int(count) == 0
    finally:
        connection.close()

    # Setup the config object
    config = proxysql_tools_config(proxysql_instance,
                                   percona_xtradb_cluster_node.host,
                                   percona_xtradb_cluster_node.port,
                                   percona_xtradb_cluster_node.user,
                                   percona_xtradb_cluster_node.password,
                                   hostgroup_writer, hostgroup_reader,
                                   'monitor', 'monitor')

    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)

    runner = CliRunner()
    result = runner.invoke(main, ['--config', config_file,
                                  'galera', 'register'])
    assert result.exit_code == 0

    connection = pymysql.connect(host=proxysql_instance.host, port=proxysql_instance.port,
                           user=proxysql_instance.user, passwd=proxysql_instance.password,
                           connect_timeout=20,
                           cursorclass=DictCursor)
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM mysql_servers '
                           ' WHERE hostgroup_id = %s ', hostgroup_writer)
            count = cursor.fetchone()[0]
            assert int(count) == 1
            cursor.execute('SELECT COUNT(*) FROM mysql_servers '
                           ' WHERE hostgroup_id = %s ', hostgroup_reader)
            count = cursor.fetchone()[0]
            assert int(count) == 1
    finally:
        connection.close()
