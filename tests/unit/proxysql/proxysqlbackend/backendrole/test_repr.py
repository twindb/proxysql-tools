import pytest

from proxysql_tools.proxysql.proxysqlbackend import BackendRole


@pytest.mark.parametrize('role, role_repr', [
    (
        BackendRole(),
        '{"reader": false, "writer": false}'
    ),
    (
        BackendRole(writer=True),
        '{"reader": false, "writer": true}'
    )
])
def test_repr(role, role_repr):
    assert repr(role) == role_repr
