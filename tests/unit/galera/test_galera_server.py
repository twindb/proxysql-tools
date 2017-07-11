import mock
from proxysql_tools.galera.server import server_status, server_set_wsrep_desync
from proxysql_tools.proxysql.proxysql import ProxySQL


@mock.patch.object(ProxySQL, 'find_backends')
def test_server_status(config):
    server_status(config)
