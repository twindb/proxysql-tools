from pprint import pprint

import pytest

from proxysql_tools.proxysql.backendrole import BackendRole
from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend

from proxysql_tools.proxysql.fsm.state import ProxySQLState
from proxysql_tools.proxysql.proxysqlbackendset import ProxySQLMySQLBackendSet


# noinspection LongLine
@pytest.mark.parametrize('n_nodes, states', [
    (
        1,
        [
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False))],
        ]
    ),
    (
        2,
        [
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False))],
        ]
    ),
    (
        3,
        [
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=True)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=True, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=True))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=True, writer=False))],
            [ProxySQLMySQLBackend('foo0', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo1', role=BackendRole(reader=False, writer=False)), ProxySQLMySQLBackend('foo2', role=BackendRole(reader=False, writer=False))]
        ]
    )
])
def test_states(n_nodes, states):
    bs = ProxySQLMySQLBackendSet()
    for i in xrange(0, n_nodes):
        bs.add(ProxySQLMySQLBackend('foo%d' % i))

    pprint(ProxySQLState(bs).states())
    # This is to generate expected result
    # for row in ProxySQLState(bs).states():
    #     print('[{node0}, {node1}, {node2}],'.format(
    #         node0="ProxySQLMySQLBackend('%s', role=BackendRole(reader=%s, writer=%s))" % (row[0].hostname, row[0].role.is_reader(), row[0].role.is_writer()),
    #         node1="ProxySQLMySQLBackend('%s', role=BackendRole(reader=%s, writer=%s))" % (row[1].hostname, row[1].role.is_reader(), row[1].role.is_writer()),
    #         node2="ProxySQLMySQLBackend('%s', role=BackendRole(reader=%s, writer=%s))" % (row[2].hostname, row[2].role.is_reader(), row[2].role.is_writer()),
    #     ))
    assert len(ProxySQLState(bs).states()) == pow(4, n_nodes)
    assert ProxySQLState(bs).states() == states
