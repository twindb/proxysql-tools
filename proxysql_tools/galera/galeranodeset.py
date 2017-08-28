"""Class GaleraNodeSet implementation."""
from proxysql_tools.galera.exceptions import GaleraClusterNodeNotFound
from proxysql_tools.galera.galera_node import GaleraNode
from proxysql_tools.proxysql.backendset import BackendSet


class GaleraNodeSet(BackendSet):
    """Set for galera nodes"""

    def __contains__(self, item):
        if isinstance(item, GaleraNode):
            return item in self._backend_list
        elif isinstance(item, GaleraNodeSet):
            return super(GaleraNodeSet, self).__contains__(item)
        return False

    def find(self, host=None, port=3306, state=None):
        """
        Find node by host and port or state

        :param host: Hostname of backend
        :param port: Port of backend
        :param state: State of node
        :type state:
        :return: Return found node
        :raises: GaleraClusterNodeNotFound
        """
        nodes = []
        for node in self._backend_list:
            if all((
                    # A -> B
                    # Statement is False if
                    # A is True
                    # B is False
                    # Otherwise True
                    # See https://en.wikipedia.org/wiki/Material_conditional
                    not host or node.host == host,
                    not port or node.port == port,
                    not state or node.wsrep_local_state == state)):
                nodes.append(node)
        if nodes:
            return nodes
        raise GaleraClusterNodeNotFound('Node not found')

    def remove(self, backend):
        """Remove node from the set

        :param backend: Node to remove.
        :type backend: GaleraNode
        :raise GaleraClusterNodeNotFound: if node is not in the set
        """
        try:
            self._backend_list.remove(backend)
        except ValueError as err:
            raise GaleraClusterNodeNotFound(err)
