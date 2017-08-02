"""Module describes GaleraCluster class"""
from pymysql import OperationalError

from proxysql_tools import LOG
from proxysql_tools.galera.exceptions import GaleraClusterSyncedNodeNotFound, \
    GaleraClusterNodeNotFound
from proxysql_tools.galera.galera_node import GaleraNode, GaleraNodeState


class GaleraCluster(object):
    """
    GaleraCluster describes Galera cluster.

    :param cluster_hosts: .
    :type cluster_hosts: str
    :param user: MySQL user to connect to a cluster node.
    :type user: str
    :param password: MySQL password.
    :type password: str
    """
    def __init__(self, cluster_hosts, user='root', password=None):
        self._nodes = []
        for host in self._split_cluster_host(cluster_hosts):
            self._nodes.append(GaleraNode(host=host[0], port=host[1],
                                          user=user, password=password))

    @property
    def nodes(self):
        """
        Get list of Galera nodes

        :return: Return list of Galera nodes
        :rtype: list(GaleraNode)"""
        return self._nodes

    def find_node(self, host, port):
        """
        BY given host and port find a node in the cluster.

        :param host: IP or address of node.
        :param port: node port.
        :return: GaleraNode instance.
        :rtype: GaleraNode
        :raise: GaleraClusterNodeNotFound
        """
        for node in self._nodes:
            if node.host == host and node.port == port:
                return node

        raise GaleraClusterNodeNotFound('Cannot find node %s:%d', host, port)

    def find_synced_nodes(self, use_last_desynced=None):
        """Find a node in the cluster in SYNCED state.
        :param: use_last_desynced: Use the last desynced node if needed
        :return: List of Galera node in SYNCED state.
        :rtype: list(GaleraNode)
        :raise: GaleraClusterSyncedNodeNotFound
        """
        use_last_desynced = use_last_desynced
        LOG.debug('Looking for a SYNCED node')
        nodes = []
        for galera_node in self._nodes:
            try:
                state = galera_node.wsrep_local_state
                reject_queries = galera_node.wsrep_reject_queries
                LOG.debug('%s state: %s', galera_node, state)
                if state == GaleraNodeState.SYNCED and reject_queries == "NONE":
                    nodes.append(galera_node)
            except OperationalError as err:
                LOG.error(err)
                LOG.info('Skipping node %s', galera_node)
        if nodes:
            return nodes
        else:
            if use_last_desynced:
                LOG.debug('Looking for an available DESYNCED node')
                nodes = []
                for galera_node in self._nodes:
                    try:
                        state = galera_node.wsrep_local_state
                        donor_reject = galera_node.wsrep_sst_donor_rejects_queries
                        reject_queries = galera_node.wsrep_reject_queries
                        length = len(nodes)
                        LOG.debug('%s state: %s donor reject: %s reject queries: %s',
                                  galera_node, state, donor_reject, reject_queries)
                        if state == GaleraNodeState.DONOR and length == 0 and donor_reject == "OFF" and reject_queries == "NONE":
                                nodes.append(galera_node)
                    except OperationalError as err:
                        LOG.error(err)
                        LOG.info('Skipping node %s', galera_node)
                if nodes:
                    return nodes

            raise GaleraClusterSyncedNodeNotFound('Cluster has '
                                                  'no SYNCED nodes')

    @staticmethod
    def _split_cluster_host(cluster_host):
        """Split a string with list of hosts and make a list of tuples out of it.
        For example, string
        *192.168.90.2:3306,192.168.90.3:3306,192.168.90.4:3306*
        will be converted into list:

.. code-block:: python

    [
        (192.168.90.2, 3306),
        (192.168.90.3, 3306),
        (192.168.90.4, 3306),
    ]


:param cluster_host: String with list of host:port pairs
:type cluster_host: str
:return: list of tuples (host, port)
:rtype: list(tuple)
        """
        result = []
        for item in cluster_host.split(','):
            host, port = item.split(':')
            result.append((host, int(port)))

        return result
