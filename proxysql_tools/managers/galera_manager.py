from schematics.exceptions import ModelValidationError

from pymysql.err import OperationalError

from proxysql_tools import log
from proxysql_tools.entities.galera import GaleraNode, CLUSTER_STATUS_PRIMARY


class GaleraManager(object):
    def __init__(self, cluster_node_host, cluster_node_port, user, password):
        """Initializes the Galera manager.

        :param cluster_node_host: The Galera cluster host to operate against.
        :type cluster_node_host: str
        :param cluster_node_port: The MySQL port on Galera cluster host to
            connect to.
        :type cluster_node_port: int
        :param user: The MySQL username.
        :type user: str
        :param password: The MySQL password.
        :type password: str
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

        :return: Returns True on success, False otherwise.
        :rtype: bool
        """
        initial_node = GaleraNode({
            'host': self.host,
            'port': self.port,
            'username': self.user,
            'password': self.password
        })

        # Check that the initial node status is 'PRIMARY'
        self.refresh_and_validate_node_state(initial_node)

        self._nodes = [initial_node]

        with initial_node.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW GLOBAL STATUS LIKE "
                               "'wsrep_incoming_addresses'")
                res = {r['Variable_name'].lower(): r['Value'].lower()
                       for r in cursor.fetchall()}

                if not res.get('wsrep_incoming_addresses'):
                    err_msg = ('Node %s:%s unknown status variable '
                               '"wsrep_incoming_addresses"')

                    log.error(err_msg)
                    raise GaleraNodeUnknownState(err_msg)

                log.info('Node %s:%s wsrep_incoming_addresses:: %s' %
                         (initial_node.host, initial_node.port,
                          res['wsrep_incoming_addresses']))

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

                    # Check that the initial node status is 'PRIMARY'
                    self.refresh_and_validate_node_state(node)

                    if GaleraManager.nodes_in_same_cluster(initial_node, node):
                        self._nodes.append(node)

        return True

    @staticmethod
    def refresh_and_validate_node_state(node):
        """Validates the state of the node to ensure that the node's status
        is PRIMARY and that all the node's properties can be fetched.

        :param node: The object that stores information on the Galera node.
        :type node: GaleraNode
        """
        try:
            node.refresh_state()
            if not node.cluster_status == CLUSTER_STATUS_PRIMARY:
                err_msg = ('Node %s:%s is in non-primary '
                           'state %s' % (node.host, node.port,
                                         node.cluster_status))

                log.error(err_msg)
                raise GaleraNodeNonPrimary(err_msg)
        except (ModelValidationError, OperationalError) as e:
            # The node state cannot be refreshed as some of the
            # properties of the node could not be fetched. We
            # should fail the discovery in such a case as this is
            # unexpected error.
            log.error('Node %s:%s state could not be fetched' %
                      (node.host, node.port))
            raise GaleraNodeUnknownState(e.messages)

    @staticmethod
    def nodes_in_same_cluster(node1, node2):
        """Check to see if the two nodes belong to the same cluster.

        :param node1: The Galera node to be compared.
        :type node1: GaleraNode
        :param node2: The Galera node to be compared.
        :type node2: GaleraNode
        :return: Returns True if both nodes are in the same cluster,
            False otherwise.
        :rtype: bool
        """
        return node1.cluster_state_uuid == node2.cluster_state_uuid


class GaleraNodeUnknownState(Exception):
    pass


class GaleraNodeNonPrimary(Exception):
    pass
