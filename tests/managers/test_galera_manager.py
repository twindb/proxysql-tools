def test__can_connect_to_galera_node(galera_manager):
    with galera_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT VERSION() AS version")
            result = cursor.fetchone()

            version = result['version']

    assert "5.7" in version
