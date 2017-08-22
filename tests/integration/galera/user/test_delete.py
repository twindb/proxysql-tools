import pymysql
from click.testing import CliRunner
from pymysql.cursors import DictCursor

import proxysql_tools
from proxysql_tools.cli import main
from tests.integration.library import proxysql_tools_config

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
