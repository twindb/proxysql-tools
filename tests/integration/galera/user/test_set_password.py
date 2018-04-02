import pymysql
from click.testing import CliRunner
from pymysql.cursors import DictCursor

import proxysql_tools
from proxysql_tools.cli import main
from proxysql_tools.proxysql.proxysql import ProxySQLMySQLBackend, ProxySQLMySQLUser
from tests.integration.library import wait_for_cluster_nodes_to_become_healthy, proxysql_tools_config_2, \
    proxysql_tools_config


def test__galera_user_set_password_if_user_is_exist(percona_xtradb_cluster_three_node,
                                                    proxysql_instance, tmpdir):
    wait_for_cluster_nodes_to_become_healthy(percona_xtradb_cluster_three_node)
    hostgroup_writer = 10
    hostgroup_reader = 11

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
