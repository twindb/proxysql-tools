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

    found_nodes = bs.find(**criteria)
    assert len(found_nodes) == 1
    assert result in found_nodes
    assert isinstance(found_nodes, GaleraNodeSet)


def test_index_slice():
    bs = GaleraNodeSet()
    bs.add(GaleraNode(host='foo'))
    bs.add(GaleraNode(host='bar'))

    assert len(bs) == 2
    assert bs[0] == GaleraNode(host='foo')
    assert bs[1] == GaleraNode(host='bar')

    l = [1, 2, 3]
    assert l[:1] == [1]
    slice_set = GaleraNodeSet()
    slice_set.add(GaleraNode(host='foo'))
    assert bs[:1] == slice_set
