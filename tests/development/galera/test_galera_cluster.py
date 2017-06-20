def test_galera_cluster(galera_cluster):
    for node in galera_cluster.nodes:
        assert node.wsrep_cluster_status == 'Primary'

