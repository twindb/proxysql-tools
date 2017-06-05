import pytest

from proxysql_tools.proxysql.proxysql import ProxySQL


@pytest.fixture
def proxysql():
    return ProxySQL()
