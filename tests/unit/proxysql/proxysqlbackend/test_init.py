import pytest

from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend, \
    BackendStatus, BackendRole


@pytest.mark.parametrize('kwargs, hostgroup_id, admin_status, role', [
    (
        {
            'hostgroup_id': 10,
            'port': 3309,
        },
        10,
        None,
        None
    ),
    (
        {
            'hostgroup_id': 10,
            'port': 3309,
            'status': 'ONLINE',
            'weight': 100,
            'compression': 1,
            'max_connections': 200,
            'max_replication_lag': 300,
            'use_ssl': True,
            'max_latency_ms': 400,
            'comment': '{"admin_status": "ONLINE", "role": "Reader"}'
        },
        10,
        BackendStatus.online,
        BackendRole.reader
    ),
    (
        {
            'hostgroup_id': 10,
            'port': 3309,
            'status': 'ONLINE',
            'weight': 100,
            'compression': 1,
            'max_connections': 200,
            'max_replication_lag': 300,
            'use_ssl': True,
            'max_latency_ms': 400,
            'comment': '{"admin_status": "OFFLINE_SOFT", "role": "Writer"}'
        },
        10,
        BackendStatus.offline_soft,
        BackendRole.writer
    ),
    (
        {
            'hostgroup_id': 10,
            'port': 3309,
            'status': 'ONLINE',
            'weight': 100,
            'compression': 1,
            'max_connections': 200,
            'max_replication_lag': 300,
            'use_ssl': True,
            'max_latency_ms': 400,
            'comment': 'Writer'
        },
        10,
        BackendStatus.online,
        BackendRole.writer
    ),
    (
        {
            'hostgroup_id': 10,
            'port': 3309,
            'status': 'OFFLINE_SOFT',
            'weight': 100,
            'compression': 1,
            'max_connections': 200,
            'max_replication_lag': 300,
            'use_ssl': True,
            'max_latency_ms': 400,
            'comment': 'Reader'
        },
        10,
        BackendStatus.offline_soft,
        BackendRole.reader
    )
])
def test_init(kwargs, hostgroup_id, admin_status, role):
    backend = ProxySQLMySQLBackend('foo', **kwargs)
    assert backend.hostgroup_id == hostgroup_id
    assert backend.admin_status == admin_status
    assert backend.role == role
