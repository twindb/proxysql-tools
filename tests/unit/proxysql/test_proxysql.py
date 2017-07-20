import mock
import pytest
from pymysql import OperationalError
from pymysql.cursors import DictCursor

from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound, ProxySQLUserNotFound
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
    mu = ProxySQLMySQLUser(username='foo',
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
    assert mu.username == 'foo'
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


def test_proxysql_mysql_user_with_type_conversion():
    mu = ProxySQLMySQLUser(username='foo',
                           password='qwerty',
                           active='0',
                           use_ssl='0',
                           default_hostgroup='10',
                           default_schema='bar',
                           schema_locked='0',
                           transaction_persistent='0',
                           fast_forward='0',
                           backend='0',
                           frontend=0,
                           max_connections='10')
    assert mu.username == 'foo'
    assert mu.password == 'qwerty'
    assert mu.active is False
    assert mu.use_ssl is False
    assert mu.default_hostgroup == 10
    assert mu.default_schema == 'bar'
    assert mu.schema_locked is False
    assert mu.transaction_persistent is False
    assert mu.fast_forward is False
    assert mu.backend is False
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


@pytest.mark.parametrize('response',[
    (
        [{
            u'username': 'foo',
            u'password': '',
            u'active': False,
            u'use_ssl': False,
            u'default_hostgroup': 0,
            u'default_schema': 'information_schema',
            u'schema_locked': False,
            u'transaction_persistent': False,
            u'fast_forward': False,
            u'backend': False,
            u'frontend': True,
            u'max_connections': '10000'
        }]
    )
])
@mock.patch.object(ProxySQL, 'execute')
def test_get_users(mock_execute, proxysql, response):
    query = "SELECT * FROM mysql_users;"
    mock_execute.return_value = response
    proxysql.get_users()
    mock_execute.assert_called_once_with(query)


@pytest.mark.parametrize('query',[
    (
        "REPLACE INTO mysql_users(`username`, `password`, `active`, `use_ssl`, `default_hostgroup`, `default_schema`, `schema_locked`, `transaction_persistent`, `fast_forward`, `backend`, `frontend`, `max_connections`) VALUES('foo', '', 1, 0, 0, 'information_schema', 0, 0, 0, 1, 1, 10000)"
    )
])
@mock.patch.object(ProxySQL, 'reload_runtime')
@mock.patch.object(ProxySQL, 'execute')
def test_add_user(mock_execute, mock_runtime, query, proxysql):
    user = ProxySQLMySQLUser(username='foo', password='')
    proxysql.add_user(user)
    mock_execute.assert_called_once_with(query)
    mock_runtime.assert_called_once_with()


@mock.patch.object(ProxySQL, 'reload_runtime')
@mock.patch.object(ProxySQL, 'execute')
def test_delete_user(mock_execute, mock_runtime, proxysql):
    user = ProxySQLMySQLUser(username='foo', password='bar')
    proxysql.delete_user('test')
    query = "DELETE FROM mysql_users WHERE username='test'"
    mock_execute.assert_called_once_with(query)
    mock_runtime.assert_called_once_with()

@mock.patch.object(ProxySQL, 'execute')
def test_get_user(mock_execute, proxysql):
    user = ProxySQLMySQLUser(username='foo', password='bar')
    proxysql.get_user('test')
    query = "SELECT * FROM mysql_users WHERE username = 'test'"
    mock_execute.assert_called_once_with(query)


@mock.patch.object(ProxySQL, 'execute')
def test_get_user_if_user_does_not_exist(mock_execute, proxysql):
    user = ProxySQLMySQLUser(username='foo', password='bar')
    mock_execute.return_value = []
    with pytest.raises(ProxySQLUserNotFound):
        proxysql.get_user('test')

    query = "SELECT * FROM mysql_users WHERE username = 'test'"
    mock_execute.assert_called_once_with(query)
