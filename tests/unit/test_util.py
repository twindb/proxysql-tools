import pytest
from mock import mock_open, mock
from pymysql import OperationalError

from proxysql_tools.util.bug1258464 import get_my_cnf, get_pid, bug1258464


def test__get_my_cnf_when_dist_not_supported(mocker):
    mock_func = mocker.patch('proxysql_tools.util.bug1258464.platform').linux_distribution
    mock_func.return_value = ['0']
    with pytest.raises(NotImplementedError):
        get_my_cnf()


def test__get_my_cnf_when_dist_is_deb_based(mocker):
    mock_func = mocker.patch('proxysql_tools.util.bug1258464.platform').linux_distribution
    mock_func.return_value = ['debian']
    assert get_my_cnf() == '/etc/mysql/my.cnf'


def test__get_my_cnf_when_dist_is_rhel_based(mocker):
    mock_func = mocker.patch('proxysql_tools.util.bug1258464.platform').linux_distribution
    mock_func.return_value = ['RHEL']
    assert get_my_cnf() == '/etc/my.cnf'


def test__get_pid_returns_integer_value(mocker):
    mock_func = mocker.patch('proxysql_tools.util.bug1258464.ConfigParser').get
    mock_func.return_value = 'some path'
    mocker.patch('proxysql_tools.util.bug1258464.open', mock_open(read_data='3333'))
    assert type(get_pid('foo-bar')) is int

def test__bug1258464_when_condition_for_kill_is_true(mocker):
    mock_pymysql = mocker.patch('proxysql_tools.util.bug1258464.pymysql')
    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = [101]
    mock_pymysql.connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor
    mock_get_my_cnf = mocker.patch('proxysql_tools.util.bug1258464.get_my_cnf')
    mock_get_my_cnf.return_value = 'some path'
    mock_get_pid = mocker.patch('proxysql_tools.util.bug1258464.get_pid')
    mock_get_pid.return_value = 300
    mock_kill_process = mocker.patch('proxysql_tools.util.bug1258464.kill_process')
    assert bug1258464('some path')


def test__bug1258464_when_condition_for_kill_is_false(mocker):
    mock_pymysql = mocker.patch('proxysql_tools.util.bug1258464.pymysql')
    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = [100]
    mock_pymysql.connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor
    mock_get_my_cnf = mocker.patch('proxysql_tools.util.bug1258464.get_my_cnf')
    mock_get_my_cnf.return_value = 'some path'
    mock_get_pid = mocker.patch('proxysql_tools.util.bug1258464.get_pid')
    mock_get_pid.return_value = 300
    mock_kill_process = mocker.patch('proxysql_tools.util.bug1258464.kill_process')
    assert not bug1258464('some path')


def test__bug1258464_when_raise_NotImplementedError(mocker):
    mock_pymysql = mocker.patch('proxysql_tools.util.bug1258464.pymysql')
    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = [100]
    mock_pymysql.connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor
    mock_get_my_cnf = mocker.patch('proxysql_tools.util.bug1258464.get_my_cnf')
    mock_get_my_cnf.return_value = 'some path'
    mock_get_my_cnf.side_effect = NotImplementedError
    assert not bug1258464('some path')


def test__bug1258464_when_raise_OSError(mocker):
    mock_pymysql = mocker.patch('proxysql_tools.util.bug1258464.pymysql')
    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = [100]
    mock_pymysql.connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor
    mock_get_my_cnf = mocker.patch('proxysql_tools.util.bug1258464.get_my_cnf')
    mock_get_my_cnf.return_value = 'some path'
    mock_get_pid = mocker.patch('proxysql_tools.util.bug1258464.get_pid')
    mock_get_pid.side_effect = OSError
    assert not bug1258464('some path')


def test__bug1258464_when_raise_OperationalError(mocker):
    mock_pymysql = mocker.patch('proxysql_tools.util.bug1258464.pymysql')
    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = [100]
    mock_pymysql.connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor
    mock_pymysql.connect.side_effect = OperationalError
    assert not bug1258464('some path')
