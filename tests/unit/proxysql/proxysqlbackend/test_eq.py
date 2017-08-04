from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend


def test_beackends_equal():
    ba = ProxySQLMySQLBackend('foo')
    bb = ProxySQLMySQLBackend('foo')
    assert ba == bb
