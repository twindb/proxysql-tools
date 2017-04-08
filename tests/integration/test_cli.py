from click.testing import CliRunner

from proxysql_tools.cli import main
from proxysql_tools.managers.galera_manager import GaleraManager
from tests.integration.library import proxysql_tools_config


def test__main_command_version_can_be_fetched():
    runner = CliRunner()
    result = runner.invoke(main, ['--version'])
    assert result.exit_code == 0


def test__ping_command_can_be_executed(proxysql_manager, tmpdir):
    config = proxysql_tools_config(proxysql_manager, '127.0.0.1', '3306',
                                   'user', 'pass', 10, 11, 'monitor',
                                   'monitor')
    config_file = str(tmpdir.join('proxysql-tool.cfg'))
    with open(config_file, 'w') as fh:
        config.write(fh)

    runner = CliRunner()
    result = runner.invoke(main, ['--config', config_file, 'ping'])
    assert result.exit_code == 0


def test__galera_register_command_can_register_cluster_with_proxysql(
        percona_xtradb_cluster_node, proxysql_manager, tmpdir):
    hostgroup_writer = 10
    hostgroup_reader = 11

    # To start off there should be no nodes in the hostgroups
    assert len(proxysql_manager.fetch_backends(hostgroup_writer)) == 0
    assert len(proxysql_manager.fetch_backends(hostgroup_reader)) == 0

    galera_man = GaleraManager(
        percona_xtradb_cluster_node.host, percona_xtradb_cluster_node.port,
        percona_xtradb_cluster_node.username,
        percona_xtradb_cluster_node.password
    )
    galera_man.discover_cluster_nodes()

    # Validate that there is one healthy node in the cluster
    assert len(galera_man.nodes) == 1

    # Setup the config object
    config = proxysql_tools_config(proxysql_manager,
                                   percona_xtradb_cluster_node.host,
                                   percona_xtradb_cluster_node.port,
                                   percona_xtradb_cluster_node.username,
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

    writer_backends = proxysql_manager.fetch_backends(hostgroup_writer)
    reader_backends = proxysql_manager.fetch_backends(hostgroup_reader)

    assert len(writer_backends) == len(reader_backends) == 1
    assert writer_backends[0].hostname == reader_backends[0].hostname
    assert writer_backends[0].port == reader_backends[0].port
    assert writer_backends[0].status == reader_backends[0].status
