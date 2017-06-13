from click.testing import CliRunner
from pymysql.cursors import DictCursor

import proxysql_tools
from proxysql_tools.cli import main
from tests.integration.library import proxysql_tools_config, proxysql_tools_config_2, \
    wait_for_cluster_nodes_to_become_healthy
import pymysql
import time


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


def test__registered_three_nodes_are_online(proxysql_instance, tmpdir,
                                            percona_xtradb_cluster_three_node):


    wait_for_cluster_nodes_to_become_healthy(percona_xtradb_cluster_three_node)

    blacklist = '{}:3306'.format(percona_xtradb_cluster_three_node[2]['ip'])
    nodes = [percona_xtradb_cluster_three_node[0]['ip'] + ':3306',
             percona_xtradb_cluster_three_node[1]['ip'] + ':3306',
             percona_xtradb_cluster_three_node[2]['ip'] + ':3306'
             ]

    config = proxysql_tools_config_2(proxysql_instance,
                                   nodes,
                                   'user', 'pass', 10, 11, blacklist,'monitor',
                                   'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)
        proxysql_tools.LOG.debug('proxysql-tools config: \n%s', config)
    runner = CliRunner()
    result = runner.invoke(main, ['--config', config_file, 'galera', 'register'])

    assert result.exit_code == 0

