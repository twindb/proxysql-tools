def test_galera_node_cluster_state_uuid(galera_node1):
    assert len(galera_node1.wsrep_cluster_state_uuid) == 36


def test_galera_node_cluster_status(galera_node1):
    assert galera_node1.wsrep_cluster_status == 'Primary'


def test_galera_node_wsrep_cluster_name(galera_node1):
    assert galera_node1.wsrep_cluster_name == 'LefredPXC'
