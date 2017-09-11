from proxysql_tools.proxysql.backendrole import BackendRole


def test_is_reader():
    backend = BackendRole(reader=True)
    assert backend.is_reader()
