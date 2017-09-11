from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend


def test_repr():
    result = '{"_admin_status": null, "_connection": null, "comment": null, ' \
             '"compression": 0, "hostgroup_id": 0, "hostname": "foo", ' \
             '"max_connections": 10000, "max_latency_ms": 0, ' \
             '"max_replication_lag": 0, "port": 3306, ' \
             '"role": {"reader": false, "writer": false}, ' \
             '"status": "ONLINE", "use_ssl": false, "weight": 1}'
    assert repr(ProxySQLMySQLBackend('foo')) == result
