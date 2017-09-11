from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend

from proxysql_tools.proxysql.proxysqlbackendset import ProxySQLMySQLBackendSet


def test_repr_empty():
    assert repr(ProxySQLMySQLBackendSet()) == '[]'


# noinspection LongLine
def test_repr():
    bs = ProxySQLMySQLBackendSet()
    bs.add(ProxySQLMySQLBackend('foo'))
    assert repr(bs) == '[{"_admin_status": null, "_connection": null, "comment": null, "compression": 0, "hostgroup_id": 0, "hostname": "foo", "max_connections": 10000, "max_latency_ms": 0, "max_replication_lag": 0, "port": 3306, "role": {"reader": false, "writer": false}, "status": "ONLINE", "use_ssl": false, "weight": 1}]'

    bs.add(ProxySQLMySQLBackend('bar'))
    assert repr(bs) == '[{"_admin_status": null, "_connection": null, "comment": null, "compression": 0, "hostgroup_id": 0, "hostname": "foo", "max_connections": 10000, "max_latency_ms": 0, "max_replication_lag": 0, "port": 3306, "role": {"reader": false, "writer": false}, "status": "ONLINE", "use_ssl": false, "weight": 1}, {"_admin_status": null, "_connection": null, "comment": null, "compression": 0, "hostgroup_id": 0, "hostname": "bar", "max_connections": 10000, "max_latency_ms": 0, "max_replication_lag": 0, "port": 3306, "role": {"reader": false, "writer": false}, "status": "ONLINE", "use_ssl": false, "weight": 1}]'
