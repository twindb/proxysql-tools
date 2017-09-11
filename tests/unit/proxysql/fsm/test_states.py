from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend

from proxysql_tools.proxysql.fsm.state import ProxySQLState
from proxysql_tools.proxysql.proxysqlbackendset import ProxySQLMySQLBackendSet


def test_states():
    bs = ProxySQLMySQLBackendSet()
    for i in [1, 2, 3]:
        bs.add(ProxySQLMySQLBackend('foo%d' % i))

    print(ProxySQLState(bs).states())
    assert False
