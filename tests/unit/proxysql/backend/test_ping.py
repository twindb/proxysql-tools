import mock
from pymysql import OperationalError

from proxysql_tools.proxysql.proxysql import ProxySQL


@mock.patch.object(ProxySQL, 'execute')
def test_ping_alive(mock_execute, proxysql):
    mock_execute.return_value = [{
        'result': '1'
    }]
    assert proxysql.ping() is True
    mock_execute.assert_called_once_with('SELECT 1 AS result')


@mock.patch.object(ProxySQL, 'execute')
def test_ping_dead(mock_execute, proxysql):
    mock_execute.side_effect = OperationalError
    assert proxysql.ping() is False
    mock_execute.assert_called_once_with('SELECT 1 AS result')
