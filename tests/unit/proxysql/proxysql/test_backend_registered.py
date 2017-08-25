from mock import mock

from proxysql_tools.proxysql.proxysql import ProxySQL
from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend

@mock.patch.object(ProxySQL, 'execute')
def test_backend_registered(mock_execute, proxysql):
    backend = ProxySQLMySQLBackend('127.0.0.1')
    query = "SELECT `hostgroup_id`, `hostname`, `port` " \
            "FROM `mysql_servers` " \
            "WHERE hostgroup_id = %d  " \
            "AND `hostname` = '%s'  " \
            "AND `port` = %s"  % \
            (
                int(backend.hostgroup_id),
                backend.hostname,
                int(backend.port)
            )
    proxysql.backend_registered(backend)
    mock_execute.assert_called_once_with(query)

