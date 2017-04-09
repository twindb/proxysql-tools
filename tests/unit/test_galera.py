import pytest

from doubles import allow

from proxysql_tools.entities.galera import (
    GaleraConfig,
    GaleraNode,
    LOCAL_STATE_SYNCED,
    LOCAL_STATE_DONOR_DESYNCED
)
from proxysql_tools.entities.proxysql import (
    ProxySQLMySQLBackend,
    BACKEND_STATUS_ONLINE,
    BACKEND_STATUS_OFFLINE_SOFT
)
from proxysql_tools.galera import (
    fetch_galera_manager,
    deregister_unhealthy_backends
)
from proxysql_tools.managers.proxysql_manager import ProxySQLManager


def test__fetch_galera_manager_can_create_galera_manager_object_with_valid_config(mocker):  # NOQA
    galera_config = GaleraConfig({
        'writer_hostgroup_id': 10,
        'reader_hostgroup_id': 11,
        'cluster_host': '192.168.30.51:3306',
        'cluster_username': 'cluster_user',
        'cluster_password': 'cluster_password',
        'load_balancing_mode': 'singlewriter'
    })

    mock_func = mocker.patch('proxysql_tools.galera.GaleraManager').discover_cluster_nodes  # NOQA
    mock_func.return_value = True

    assert fetch_galera_manager(galera_config)


def test__fetch_galera_manager_raises_exception_on_incorrect_cluster_host_config():  # NOQA
    galera_config = GaleraConfig({
        'writer_hostgroup_id': 10,
        'reader_hostgroup_id': 11,
        'cluster_host': '192.168.30.51_3306',
        'cluster_username': 'cluster_user',
        'cluster_password': 'cluster_password',
        'load_balancing_mode': 'singlewriter'
    })

    with pytest.raises(ValueError):
        fetch_galera_manager(galera_config)


def test__deregister_unhealthy_backends_deregisters_unhealthy_backends():
    healthy_node = GaleraNode({
        'host': '192.168.10.1',
        'port': 3306,
        'local_state': LOCAL_STATE_SYNCED
    })

    unhealthy_node = GaleraNode({
        'host': '192.168.10.2',
        'port': 3306,
        'local_state': LOCAL_STATE_DONOR_DESYNCED
    })

    healthy_backend = ProxySQLMySQLBackend({
        'status': BACKEND_STATUS_ONLINE,
        'hostname': '192.168.10.1',
        'port': 3306
    })

    unhealthy_backend = ProxySQLMySQLBackend({
        'status': BACKEND_STATUS_ONLINE,
        'hostname': '192.168.10.2',
        'port': 3306
    })

    galera_nodes = [healthy_node, unhealthy_node]
    backends = [healthy_backend, unhealthy_backend]
    hostgroup_id = 11

    proxysql_man = ProxySQLManager(host='127.0.0.1', port=6032, user='username',
                                   password='password')

    (allow(proxysql_man)
        .fetch_backends
        .with_args(hostgroup_id)
        .and_return(backends))

    (allow(proxysql_man)
        .update_mysql_backend_status
        .with_args(hostgroup_id, unhealthy_backend.hostname,
                   unhealthy_backend.port, BACKEND_STATUS_OFFLINE_SOFT)
        .and_return(True))

    backends_list = deregister_unhealthy_backends(proxysql_man, galera_nodes,
                                                  hostgroup_id,
                                                  [LOCAL_STATE_SYNCED])

    assert len(backends_list) == 1
    assert backends_list.pop() == healthy_backend
