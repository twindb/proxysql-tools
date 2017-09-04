import mock

from proxysql_tools.galera.server import server_status, server_set_wsrep_status
from proxysql_tools.proxysql.proxysql import ProxySQL


@mock.patch.object(ProxySQL, 'find_backends')
def test_server_status(mocker_find_bknd, proxysql_mysql_backend, config):
    mocker_find_bknd.return_value = [proxysql_mysql_backend]
    server_status(config)
