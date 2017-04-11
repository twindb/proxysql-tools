import pytest

from doubles import allow, expect

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
    deregister_unhealthy_backends,
    fetch_nodes_blacklisted_for_writers
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

    proxysql_man = ProxySQLManager('127.0.0.1', 6032, 'username', 'password')

    (allow(proxysql_man)
        .fetch_backends
        .with_args(hostgroup_id)
        .and_return(backends))

    (expect(proxysql_man)
        .update_mysql_backend_status
        .with_args(hostgroup_id, unhealthy_backend.hostname,
                   unhealthy_backend.port, BACKEND_STATUS_OFFLINE_SOFT)
        .and_return(True))

    backends_list = deregister_unhealthy_backends(proxysql_man, galera_nodes,
                                                  hostgroup_id,
                                                  [LOCAL_STATE_SYNCED])

    assert len(backends_list) == 1
    assert healthy_backend in backends_list
    assert unhealthy_backend not in backends_list


def test__deregister_unhealthy_backends_deregisters_blacklisted_backends():
    healthy_node = GaleraNode({
        'host': '192.168.10.1',
        'port': 3306,
        'local_state': LOCAL_STATE_SYNCED
    })

    blacklisted_node = GaleraNode({
        'host': '192.168.10.2',
        'port': 3306,
        'local_state': LOCAL_STATE_SYNCED
    })

    healthy_backend = ProxySQLMySQLBackend({
        'status': BACKEND_STATUS_ONLINE,
        'hostname': '192.168.10.1',
        'port': 3306
    })

    blacklisted_backend = ProxySQLMySQLBackend({
        'status': BACKEND_STATUS_ONLINE,
        'hostname': '192.168.10.2',
        'port': 3306
    })

    galera_nodes = [healthy_node, blacklisted_node]
    backends = [healthy_backend, blacklisted_backend]
    hostgroup_id = 11

    proxysql_man = ProxySQLManager('127.0.0.1', 6032, 'username', 'password')

    (allow(proxysql_man)
        .fetch_backends
        .with_args(hostgroup_id)
        .and_return(backends))

    (expect(proxysql_man)
        .deregister_backend
        .with_args(hostgroup_id, blacklisted_backend.hostname,
                   blacklisted_backend.port)
        .and_return(True))

    backends_list = deregister_unhealthy_backends(proxysql_man, galera_nodes,
                                                  hostgroup_id,
                                                  [LOCAL_STATE_SYNCED],
                                                  [blacklisted_node])

    assert len(backends_list) == 1
    assert healthy_backend in backends_list
    assert blacklisted_backend not in backends_list


def test__fetch_nodes_blacklisted_for_writers_returns_correct_nodes_list():
    galera_config = GaleraConfig({
        'writer_hostgroup_id': 10,
        'reader_hostgroup_id': 11,
        'cluster_host': '192.168.30.51:3306,192.168.30.52:3306',
        'cluster_username': 'cluster_user',
        'cluster_password': 'cluster_password',
        'load_balancing_mode': 'singlewriter',
        'writer_blacklist': '192.168.30.52:3306'
    })

    regular_node = GaleraNode({
        'host': '192.168.30.51',
        'port': 3306,
        'local_state': LOCAL_STATE_SYNCED
    })

    blacklist_node = GaleraNode({
        'host': '192.168.30.52',
        'port': 3306,
        'local_state': LOCAL_STATE_SYNCED
    })

    galera_nodes = [regular_node, blacklist_node]

    blacklist_nodes = fetch_nodes_blacklisted_for_writers(galera_config,
                                                          galera_nodes)

    assert len(blacklist_nodes) == 1
    assert regular_node not in blacklist_nodes
    assert blacklist_node in blacklist_nodes
