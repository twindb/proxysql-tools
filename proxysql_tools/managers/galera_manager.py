from proxysql_tools.entities.galera import GaleraNode, CLUSTER_STATUS_PRIMARY
from schematics.exceptions import ModelValidationError


class GaleraManager(object):
    def __init__(self, cluster_node_host, cluster_node_port, user, password):
        """Initializes the Galera manager.

        :param str cluster_node_host: The Galera cluster host to operate
            against.
        :param int cluster_node_port: The MySQL port on Galera cluster host to
            connect to.
        :param str user: The MySQL username.
        :param str password: The MySQL password.
        """
        self.host = cluster_node_host
        self.port = int(cluster_node_port)
        self.user = user
        self.password = password
        self._nodes = []

    @property
    def nodes(self):
        return self._nodes

    def discover_cluster_nodes(self):
        """Given the initial node find all the other nodes in the same cluster.
        It sets up the internal nodes list which is later used to perform
        operations on the nodes or the cluster.

        :return bool: Returns True on success, False otherwise.
        """
        initial_node = GaleraNode({
            'host': self.host,
            'port': self.port,
            'username': self.user,
            'password': self.password
        })
        initial_node.refresh_state()

        # Check that the initial node status is 'PRIMARY'
        if not initial_node.cluster_status == CLUSTER_STATUS_PRIMARY:
            raise GaleraNodeNonPrimary()

        self._nodes = [initial_node]

        with initial_node.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW GLOBAL STATUS LIKE "
                               "'wsrep_incoming_addresses'")
                res = {r['Variable_name'].lower(): r['Value'].lower()
                       for r in cursor.fetchall()}

                if not res.get('wsrep_incoming_addresses'):
                    raise GaleraNodeUnknownState('Unknown status variable '
                                                 '"wsrep_incoming_addresses"')

                for host_port in res['wsrep_incoming_addresses'].split(','):
                    host, port = host_port.split(':')

                    # We ignore the initial node from wsrep_incoming_addresses
                    if initial_node.host == host:
                        continue

                    node = GaleraNode({
                        'host': host,
                        'port': int(port),
                        'username': initial_node.username,
                        'password': initial_node.password
                    })

                    try:
                        node.refresh_state()
                    except ModelValidationError as e:
                        # The node state cannot be refreshed as some of the
                        # properties of the node could not be fetched.
                        raise GaleraNodeUnknownState(e.messages)

                    if GaleraManager.nodes_in_same_cluster(initial_node, node):
                        self._nodes.append(node)

        return True

    @staticmethod
    def nodes_in_same_cluster(node1, node2):
        """Check to see if the two nodes belong to the same cluster.

        :param GaleraNode node1: The Galera node to be compared.
        :param GaleraNode node2: The Galera node to be compared.
        :return bool: Returns True if both nodes are in the same cluster,
            False otherwise.
        """
        return node1.cluster_state_uuid == node2.cluster_state_uuid


class GaleraNodeUnknownState(Exception):
    pass


class GaleraNodeNonPrimary(Exception):
    pass
