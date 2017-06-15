import mock
from pymysql.cursors import DictCursor

from proxysql_tools.galera.galera_node import GaleraNode


def test_galera_node_init(galera_node):
    assert galera_node.host == 'foo'
    assert galera_node.port == 1234
    assert galera_node.user == 'bar'
    assert galera_node.password == 'xyz'


@mock.patch.object(GaleraNode, '_status')
def test_wsrep_cluster_state_uuid(mock_status, galera_node):
    """
    :param galera_node: GaleraNode instance
    :type galera_node: GaleraNode
    """
    assert galera_node.wsrep_cluster_state_uuid
    mock_status.assert_called_once_with('wsrep_cluster_state_uuid')


@mock.patch.object(GaleraNode, '_status')
def test_wsrep_cluster_status(mock_status, galera_node):
    """
    :param galera_node: GaleraNode instance
    :type galera_node: GaleraNode
    """
    assert galera_node.wsrep_cluster_status
    mock_status.assert_called_once_with('wsrep_cluster_status')


@mock.patch.object(GaleraNode, '_status')
def test_wsrep_local_state(mock_status, galera_node):
    """
    :param galera_node: GaleraNode instance
    :type galera_node: GaleraNode
    """
    mock_status.return_value = '4'
    assert galera_node.wsrep_local_state == 4
    mock_status.assert_called_once_with('wsrep_local_state')


@mock.patch.object(GaleraNode, 'execute')
def test_wsrep_cluster_name(mock_execute, galera_node):
    """
    :param galera_node: GaleraNode instance
    :type galera_node: GaleraNode
    """
    assert galera_node.wsrep_cluster_name
    mock_execute.assert_called_once_with('SELECT @@wsrep_cluster_name')


@mock.patch.object(GaleraNode, '_connect')
def test_execute(mock_connect, galera_node):
    """
    :param galera_node: GaleraNode instance
    :type galera_node: GaleraNode
    """
    mock_connection = mock.Mock()
    mock_connect.return_value.__enter__.return_value = mock_connection
    mock_cursor = mock.Mock()
    mock_connection.cursor.return_value = mock_cursor

    galera_node.execute('foo')
    mock_connect.assert_called_once_with()
    mock_connection.cursor.assert_called_once_with()
    mock_cursor.execute.assert_called_once_with('foo')
    mock_cursor.fetchall.assert_called_once_with()


@mock.patch('proxysql_tools.galera.galera_node.pymysql')
def test_connect(mock_pymysql, galera_node):
    with galera_node._connect() as conn:
        mock_pymysql.connect.assert_called_once_with(host='foo',
                                                     port=1234,
                                                     user='bar',
                                                     passwd='xyz',
                                                     cursorclass=DictCursor)


@mock.patch.object(GaleraNode, 'execute')
def test_status(mock_execute, galera_node):
    galera_node._status('foo')
    mock_execute.assert_called_once_with('SHOW GLOBAL STATUS LIKE %s', 'foo')
