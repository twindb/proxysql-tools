import pytest

from proxysql_tools.galera.galera_node import GaleraNode


@pytest.fixture
def galera_node():
    return GaleraNode('foo', port=1234, user='bar', password='xyz')
