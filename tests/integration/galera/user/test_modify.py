import pymysql
from click.testing import CliRunner
from pymysql.cursors import DictCursor

import proxysql_tools
from proxysql_tools.cli import main
from proxysql_tools.proxysql.proxysql import ProxySQLMySQLUser
from tests.integration.library import proxysql_tools_config


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
