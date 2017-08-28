import mock
import pytest

from proxysql_tools.galera.exceptions import GaleraClusterNodeNotFound
from proxysql_tools.galera.galera_node import GaleraNode, GaleraNodeState
from proxysql_tools.galera.galeranodeset import GaleraNodeSet


def test_find_raises():
    bs = GaleraNodeSet()
    with pytest.raises(GaleraClusterNodeNotFound):
        bs.find(host='foo')


# noinspection PyUnresolvedReferences
@pytest.mark.parametrize('nodes, states, criteria, result', [
    (
        [
            GaleraNode(host='foo'),
            GaleraNode(host='bar')
        ],
        [
            None, None
        ],
        {
            'host': 'foo'
        },
        GaleraNode(host='foo'),
    ),
    (
        [
            GaleraNode(host='foo', port=3306),
            GaleraNode(host='foo', port=3307)
        ],
        [
            None, None
        ],
        {
            'host': 'foo',
            'port': 3307
        },
        GaleraNode(host='foo', port=3307),
    ),
    (
        [
            GaleraNode(host='foo', port=3306),
            GaleraNode(host='foo', port=3307)
        ],
        [
            GaleraNodeState.DONOR, GaleraNodeState.SYNCED
        ],
        {
            'host': 'foo',
            'port': 3307,
            'state': GaleraNodeState.SYNCED
        },
        GaleraNode(host='foo', port=3307),
    )
])
@mock.patch.object(GaleraNode, '_status')
def test_find_host(mock_status, states, nodes, criteria, result):
    bs = GaleraNodeSet()
    mock_status.side_effect = states
    for node in nodes:
        bs.add(node)

    assert bs.find(**criteria) == [result]
