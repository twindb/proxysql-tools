import pymysql
from click.testing import CliRunner
from pymysql.cursors import DictCursor

import proxysql_tools
from proxysql_tools.cli import main
from proxysql_tools.proxysql.proxysql import ProxySQLMySQLBackend
from proxysql_tools.proxysql.proxysqlbackend import BackendStatus
from tests.integration.library import wait_for_cluster_nodes_to_become_healthy, proxysql_tools_config_2


def test__galera_server_set_sync(percona_xtradb_cluster_three_node,
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
