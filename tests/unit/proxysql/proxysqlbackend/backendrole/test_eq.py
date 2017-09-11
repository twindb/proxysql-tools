import pytest
from proxysql_tools.proxysql.backendrole import BackendRole


@pytest.mark.parametrize('writer, reader', [
    (True, True),
    (True, False),
    (False, True),
    (False, False)
])
def test_eq(writer, reader):
    r1 = BackendRole(writer=writer, reader=reader)
    r2 = BackendRole(writer=writer, reader=reader)
    assert r1 == r2
