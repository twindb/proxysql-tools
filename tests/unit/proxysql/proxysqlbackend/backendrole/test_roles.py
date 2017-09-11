from proxysql_tools.proxysql.backendrole import BackendRole


def test_roles():
    role = BackendRole()
    assert role.roles() == [
        BackendRole(writer=True, reader=True),
        BackendRole(writer=True, reader=False),
        BackendRole(writer=False, reader=True),
        BackendRole(writer=False, reader=False)
    ]
