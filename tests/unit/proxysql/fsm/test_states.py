from pprint import pprint

import pytest

from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend

from proxysql_tools.proxysql.fsm.state import ProxySQLState
from proxysql_tools.proxysql.proxysqlbackendset import ProxySQLMySQLBackendSet


@pytest.mark.parametrize('n_nodes, states', [
    (
        1,
        [
            [ProxySQLMySQLBackend('foo1')]
        ]
    )
])
def test_states(n_nodes, states):
    bs = ProxySQLMySQLBackendSet()
    for i in [1]:
        bs.add(ProxySQLMySQLBackend('foo%d' % i))

    pprint(ProxySQLState(bs).states())
    print(len(ProxySQLState(bs).states()))
    # assert False
