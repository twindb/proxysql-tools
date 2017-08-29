from click.testing import CliRunner

import proxysql_tools
from proxysql_tools.cli import main
from proxysql_tools.proxysql.proxysql import ProxySQLMySQLBackend
from tests.integration.library import wait_for_cluster_nodes_to_become_healthy, proxysql_tools_config_2


def test__galera_server_status(percona_xtradb_cluster_three_node,
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
                           ['--config', config_file, 'galera', 'server', 'status']
                           )
    assert result.exit_code == 0
