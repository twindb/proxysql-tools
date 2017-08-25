"""Module describes GaleraCluster class"""
from pymysql import OperationalError

from proxysql_tools import LOG
from proxysql_tools.galera.exceptions import GaleraClusterSyncedNodeNotFound, \
    GaleraClusterNodeNotFound
from proxysql_tools.galera.galera_node import GaleraNode, GaleraNodeState
from proxysql_tools.galera.galeranodeset import GaleraNodeSet


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

        :return: Return set of Galera nodes
        :rtype: GaleraNodeSet
        """
        return GaleraNodeSet().add_set(self._nodes)

    @staticmethod
    def _split_cluster_host(cluster_host):
        """Split a string with list of hosts and make
        a list of tuples out of it.
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
