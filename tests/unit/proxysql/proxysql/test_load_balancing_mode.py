import mock
import pytest

from proxysql_tools.galera.exceptions import GaleraClusterNodeNotFound
from proxysql_tools.galera.galera_node import GaleraNode
from proxysql_tools.galera.galeranodeset import GaleraNodeSet
from proxysql_tools.load_balancing_mode import check_sst
from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound
from proxysql_tools.proxysql.proxysql import ProxySQL

@mock.patch.object(GaleraNodeSet, 'find')
@mock.patch.object(ProxySQL, 'find_backends')
def test_check_sst_runned_if_donor_does_not_exist(mock_find_backends, mock_find,
                                                  proxysql):
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
    mock_find.side_effect = GaleraClusterNodeNotFound
    node_set = GaleraNodeSet()
    node_set.add(GaleraNode(writer.hostname))
    p = mock.PropertyMock(return_value=node_set)
    type(galera_cluster).nodes = p
    mock_find_backends.side_effect = ProxySQLBackendNotFound
    check_sst(proxysql, galera_cluster, [reader, reader_2], writer)
    mock_find_backends.assert_not_called()


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
    mock_find_backends.side_effect = ProxySQLBackendNotFound
    check_sst(proxysql, galera_cluster, [reader, reader_2], writer)
    mock_find_backends.assert_called_once()
