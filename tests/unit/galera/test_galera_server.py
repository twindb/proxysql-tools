import mock
import pytest
from prettytable import PrettyTable

from proxysql_tools.galera.server import server_status, server_set_wsrep_desync
from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound
from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLBackend


@mock.patch.object(ProxySQL, 'find_backends')
def test_server_status(mocker_find_bknd, proxysql_mysql_backend, config):
    mocker_find_bknd.return_value = [proxysql_mysql_backend]
    server_status(config)


@mock.patch.object(ProxySQL, 'find_backends')
def test_server_set_wsrep_desync(mocker_find_bknd, config):
    backend = mock.MagicMock()
    backend.hostname = '127.0.0.1'
    backend.port = 3306
    backend.connect = mock.MagicMock()
    backend.execute = mock.MagicMock()
    mocker_find_bknd.return_value = [backend]
    server_set_wsrep_desync(config, '127.0.0.1', 3306)


@mock.patch.object(ProxySQL, 'find_backends')
def test_server_set_wsrep_desync_raise(mocker_find_bknd, config):
    backend = mock.MagicMock()
    backend.hostname = '127.0.0.1'
    backend.port = 3307
    backend.connect = mock.MagicMock()
    backend.execute = mock.MagicMock()
    mocker_find_bknd.return_value = [backend]
    with pytest.raises(ProxySQLBackendNotFound):
        server_set_wsrep_desync(config, '127.0.0.1', 3306)
