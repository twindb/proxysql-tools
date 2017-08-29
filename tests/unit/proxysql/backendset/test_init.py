from proxysql_tools.proxysql.proxysqlbackendset import ProxySQLMySQLBackendSet


def test_init_empty_set():
    bs = ProxySQLMySQLBackendSet()
    assert len(bs) == 0
