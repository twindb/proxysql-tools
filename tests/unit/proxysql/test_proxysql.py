import mock
import pytest
from pymysql import OperationalError
from pymysql.cursors import DictCursor

from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound
from proxysql_tools.proxysql.proxysql import BackendStatus, ProxySQLMySQLBackend, \
    ProxySQLMySQLUser, ProxySQL


def test_backendstatus():
    bs = BackendStatus()
    assert bs.online == 'ONLINE'
    assert bs.shunned == 'SHUNNED'
    assert bs.offline_hard == 'OFFLINE_HARD'
    assert bs.offline_soft == 'OFFLINE_SOFT'


def test_proxysql_mysql_backend():
    be = ProxySQLMySQLBackend('foo',
                              hostgroup_id='0',
                              port='3307',
                              status=BackendStatus.offline_hard,
                              weight='1',
                              compression='1',
                              max_connections='10',
                              max_replication_lag='20',
                              use_ssl=100,
                              max_latency_ms='30',
                              comment='bar')

    assert be.hostgroup_id == 0
    assert be.port == 3307
    assert be.status == 'OFFLINE_HARD'
    assert be.weight == 1
    assert be.compression == 1
    assert be.max_connections == 10
    assert be.max_replication_lag == 20
    assert be.use_ssl is True
    assert be.max_latency_ms == 30
    assert be.comment == 'bar'


def test_proxysql_mysql_user():
    mu = ProxySQLMySQLUser(user='foo',
                           password='qwerty',
                           active=True,
                           use_ssl=True,
                           default_hostgroup='10',
                           default_schema='bar',
                           schema_locked=True,
                           transaction_persistent=True,
                           fast_forward=True,
                           backend=True,
                           frontend=False,
                           max_connections='10')
    assert mu.user == 'foo'
    assert mu.password == 'qwerty'
    assert mu.active is True
    assert mu.use_ssl is True
    assert mu.default_hostgroup == 10
    assert mu.default_schema == 'bar'
    assert mu.schema_locked is True
    assert mu.transaction_persistent is True
    assert mu.fast_forward is True
    assert mu.backend is True
    assert mu.frontend is False
    assert mu.max_connections == 10


def test_proxysql():
    ps = ProxySQL(host='foo',
                  port='3307',
                  user='bar',
                  password='qwerty')
    assert ps.host == 'foo'
    assert ps.port == 3307
    assert ps.user == 'bar'
    assert ps.password == 'qwerty'


@mock.patch.object(ProxySQL, '_connect')
def test_execute(mock_connect, proxysql):
    """
    :param proxysql: ProxySQL instance
    :type proxysql: ProxySQL
    """
    mock_connection = mock.Mock()
    mock_connect.return_value.__enter__.return_value = mock_connection
    mock_cursor = mock.Mock()
    mock_connection.cursor.return_value = mock_cursor

    proxysql.execute('foo')
    mock_connect.assert_called_once_with()
    mock_connection.cursor.assert_called_once_with()
    mock_cursor.execute.assert_called_once_with('foo')
    mock_cursor.fetchall.assert_called_once_with()


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


@mock.patch.object(ProxySQL, 'execute')
def test_reload_runtime(mock_execute, proxysql):
    """
    :param proxysql: ProxySQL instance
    :type proxysql: ProxySQL
    """
    proxysql.reload_runtime()
    calls = [mock.call('LOAD MYSQL SERVERS TO RUNTIME'),
             mock.call('LOAD MYSQL USERS TO RUNTIME'),
             mock.call('LOAD MYSQL VARIABLES TO RUNTIME')]
    mock_execute.assert_has_calls(calls=calls, any_order=False)


@pytest.mark.parametrize('comment, query',[
    (
        'Some comment',
        "REPLACE INTO mysql_servers(`hostgroup_id`, `hostname`, `port`, `status`, `weight`, `compression`, `max_connections`, `max_replication_lag`, `use_ssl`, `max_latency_ms`, `comment`) VALUES(0, 'foo', 3306, 'ONLINE', 1, 0, 10000, 0, 0, 0, 'Some comment')"
    ),
    (
        None,
        "REPLACE INTO mysql_servers(`hostgroup_id`, `hostname`, `port`, `status`, `weight`, `compression`, `max_connections`, `max_replication_lag`, `use_ssl`, `max_latency_ms`, `comment`) VALUES(0, 'foo', 3306, 'ONLINE', 1, 0, 10000, 0, 0, 0, NULL)"
    )
])
@mock.patch.object(ProxySQL, 'reload_runtime')
@mock.patch.object(ProxySQL, 'execute')
def test_register_backend(mock_execute, mock_runtime, comment, query, proxysql):
    """

    :param mock_execute:
    :param mock_runtime:
    :param comment:
    :param query:
    :param proxysql:
    :type proxysql: ProxySQL
    """
    backend = ProxySQLMySQLBackend('foo', comment=comment)
    proxysql.register_backend(backend)
    mock_execute.assert_called_once_with(query)
    mock_runtime.assert_called_once_with()


@mock.patch.object(ProxySQL, 'reload_runtime')
@mock.patch.object(ProxySQL, 'execute')
def test_deregister_backend(mock_execute, mock_runtime, proxysql):
    """

    :param mock_execute:
    :param mock_runtime:
    :param comment:
    :param query:
    :param proxysql:
    :type proxysql: ProxySQL
    """
    backend = ProxySQLMySQLBackend('foo', hostgroup_id=10, port=3307)
    proxysql.deregister_backend(backend)
    query = "DELETE FROM mysql_servers WHERE hostgroup_id=10 AND hostname='foo' AND port=3307"
    mock_execute.assert_called_once_with(query)
    mock_runtime.assert_called_once_with()


@pytest.mark.parametrize('kwargs_in, kwargs_out', [
    (
        {
            'socket': '/foo/bar'
        },
        {
            'connect_timeout': 20,
            'cursorclass': DictCursor,
            'passwd': None,
            'unix_socket': '/foo/bar',
            'user': 'root'

        }
    ),
    (
        {
            'port': 3307
        },
        {
            'connect_timeout': 20,
            'cursorclass': DictCursor,
            'passwd': None,
            'host': 'localhost',
            'port': 3307,
            'user': 'root'

        }
    ),
    (
        {

        },
        {
            'connect_timeout': 20,
            'cursorclass': DictCursor,
            'passwd': None,
            'host': 'localhost',
            'port': 3306,
            'user': 'root'

        }
    )
])
@mock.patch('proxysql_tools.proxysql.proxysql.pymysql')
def test_connect(mock_pymysql, kwargs_in, kwargs_out):
    ps = ProxySQL(**kwargs_in)
    with ps._connect() as conn:
        mock_pymysql.connect.assert_called_once_with(**kwargs_out)


@pytest.mark.parametrize('response, backend',[
    (
        [{
            u'status': 'ONLINE',
            u'comment': '',
            u'compression': '0',
            u'weight': '1',
            u'hostname': '192.168.90.2',
            u'hostgroup_id': '10',
            u'use_ssl': '0',
            u'max_replication_lag': '0',
            u'port': '3306',
            u'max_latency_ms': '0',
            u'max_connections': '10000'
        }]
        ,
        ProxySQLMySQLBackend('192.168.90.2', hostgroup_id=10, port=3306)
    )
])
@mock.patch.object(ProxySQL, 'execute')
def test_find_backends(mock_execute, proxysql, response, backend):
    mock_execute.return_value = response
    assert proxysql.find_backends(10)[0] == backend


@mock.patch.object(ProxySQL, 'execute')
def test_find_backends_raises(mock_execute, proxysql):
    mock_execute.return_value = ()
    with pytest.raises(ProxySQLBackendNotFound):
        proxysql.find_backends(10)
