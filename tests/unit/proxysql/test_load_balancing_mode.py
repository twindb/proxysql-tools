import mock
import pytest

from proxysql_tools.galera.exceptions import GaleraClusterNodeNotFound
from proxysql_tools.galera.galera_node import GaleraNode
from proxysql_tools.galera.galera_cluster import GaleraCluster
from proxysql_tools.load_balancing_mode import check_sst_runned
from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound
from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLBackend, BackendStatus


@mock.patch.object(ProxySQL, 'set_status')
@mock.patch.object(ProxySQL, 'find_backends')
def test_check_sst_runned_if_two_nodes_online(mock_find_backends,
                                              mock_set_status, proxysql):
    reader = mock.MagicMock()
    reader.hostname = '127.0.0.1'
    reader.port = 3306
    writer = mock.MagicMock()
    writer.hostname = '127.0.0.2'
    writer.port = 3306
    galera_cluster = mock.MagicMock()
    galera_cluster.find_donor_nodes.return_value = [GaleraNode(writer.hostname)]
    mock_find_backends.side_effect = ProxySQLBackendNotFound
    check_sst_runned(proxysql, galera_cluster, [reader], writer)
    mock_set_status.assert_called_once()
    mock_find_backends.assert_called_once()

@mock.patch.object(ProxySQL, 'find_backends')
def test_check_sst_runned_if_more_then_two_nodes_online(mock_find_backends, proxysql):
    reader = mock.MagicMock()
    reader.hostname = '127.0.0.1'
    reader.port = 3306
    reader_2 = mock.MagicMock()
    reader_2.hostname = '127.0.0.1'
    reader_2.port = 3306
    writer = mock.MagicMock()
    writer.hostname = '127.0.0.2'
    writer.port = 3306
    galera_cluster = mock.MagicMock()
    galera_cluster.find_donor_nodes.return_value = [GaleraNode(writer.hostname)]
    mock_find_backends.side_effect = ProxySQLBackendNotFound
    check_sst_runned(proxysql, galera_cluster, [reader, reader_2], writer)
    galera_cluster.find_donor_nodes.assert_not_called()
    mock_find_backends.assert_called_once()


@mock.patch.object(ProxySQL, 'find_backends')
def test_check_sst_runned_if_donor_does_not_exist(mock_find_backends, proxysql):
    reader = mock.MagicMock()
    reader.hostname = '127.0.0.1'
    reader.port = 3306
    reader_2 = mock.MagicMock()
    reader_2.hostname = '127.0.0.1'
    reader_2.port = 3306
    writer = mock.MagicMock()
    writer.hostname = '127.0.0.2'
    writer.port = 3306
    galera_cluster = mock.MagicMock()
    galera_cluster.find_donor_nodes.side_effect = GaleraClusterNodeNotFound
    mock_find_backends.side_effect = ProxySQLBackendNotFound
    check_sst_runned(proxysql, galera_cluster, [reader, reader_2], writer)
    mock_find_backends.assert_called_once()
