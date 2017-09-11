import json
from pprint import pprint

from proxysql_tools.proxysql.fsm.state import ProxySQLState
from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend

from proxysql_tools.proxysql.proxysqlbackendset import ProxySQLMySQLBackendSet


def test_init():
    bs = ProxySQLMySQLBackendSet()
    for i in [1, 2, 3]:
        bs.add(ProxySQLMySQLBackend('foo%d' % i))

    pprint(json.loads(str(bs)))
    assert ProxySQLState(bs).backends
    # assert False
