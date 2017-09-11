from proxysql_tools.proxysql.backendrole import BackendRole


def test_is_writer():
    backend = BackendRole(writer=True)
    assert backend.is_writer()
