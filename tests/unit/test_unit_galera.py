import pytest

from proxysql_tools.entities.galera import GaleraConfig
from proxysql_tools.galera import fetch_galera_manager


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
