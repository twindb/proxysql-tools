import mock
import pytest

from proxysql_tools.galera.server import server_set_admin_status
from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLBackend
from proxysql_tools.util import get_proxysql_options


@mock.patch('proxysql_tools.galera.server.get_backend')
@mock.patch.object(ProxySQL, 'reload_runtime')
@mock.patch.object(ProxySQL, 'execute')
def test_server_set_admin_status(mock_execute, mock_runtime, mock_get_bknd, config):
    mock_get_bknd.return_value = ProxySQLMySQLBackend('127.0.0.1')
    server_set_admin_status(get_proxysql_options(config), '127.0.0.1', 3306)
    mock_runtime.assert_called_once()
    mock_get_bknd.assert_called_once()
