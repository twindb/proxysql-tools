import pytest

from proxysql_tools.proxysql.backendrole import BackendRole
from proxysql_tools.proxysql.fsm.state import ProxySQLState
from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend

from proxysql_tools.proxysql.proxysqlbackendset import ProxySQLMySQLBackendSet


# noinspection LongLine
@pytest.mark.parametrize('n_nodes, roles_map', [
    (
        1,
        [
            (BackendRole(reader=True, writer=True),),
            (BackendRole(reader=False, writer=True),),
            (BackendRole(reader=True, writer=False),),
            (BackendRole(reader=False, writer=False),)
        ]
    ),
    (
        2,
        [
            (BackendRole(reader=True, writer=True), BackendRole(reader=True, writer=True)),
            (BackendRole(reader=True, writer=True), BackendRole(reader=False, writer=True)),
            (BackendRole(reader=True, writer=True), BackendRole(reader=True, writer=False)),
            (BackendRole(reader=True, writer=True), BackendRole(reader=False, writer=False)),
            (BackendRole(reader=False, writer=True), BackendRole(reader=True, writer=True)),
            (BackendRole(reader=False, writer=True), BackendRole(reader=False, writer=True)),
            (BackendRole(reader=False, writer=True), BackendRole(reader=True, writer=False)),
            (BackendRole(reader=False, writer=True), BackendRole(reader=False, writer=False)),
            (BackendRole(reader=True, writer=False), BackendRole(reader=True, writer=True)),
            (BackendRole(reader=True, writer=False), BackendRole(reader=False, writer=True)),
            (BackendRole(reader=True, writer=False), BackendRole(reader=True, writer=False)),
            (BackendRole(reader=True, writer=False), BackendRole(reader=False, writer=False)),
            (BackendRole(reader=False, writer=False), BackendRole(reader=True, writer=True)),
            (BackendRole(reader=False, writer=False), BackendRole(reader=False, writer=True)),
            (BackendRole(reader=False, writer=False), BackendRole(reader=True, writer=False)),
            (BackendRole(reader=False, writer=False), BackendRole(reader=False, writer=False))
        ]
    )

])
def test_roles_map(n_nodes, roles_map):
    bs = ProxySQLMySQLBackendSet()
    for i in xrange(0, n_nodes):
        bs.add(ProxySQLMySQLBackend('foo%d' % i))

    # noinspection PyProtectedMember
    possible_roles = ProxySQLState(bs)._roles_map()
    assert possible_roles == roles_map
