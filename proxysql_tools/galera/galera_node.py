"""Module describes GaleraNode class"""
from contextlib import contextmanager

import pymysql
from pymysql.cursors import DictCursor

from proxysql_tools import execute


class GaleraNodeState(object):  # pylint: disable=too-few-public-methods
    """State of Galera node http://bit.ly/2r1tUGB """
    PRIMARY = 1
    JOINER = 2
    JOINED = 3
    SYNCED = 4
    DONOR = 5


class GaleraNode(object):
    """
    GaleraNode class describes a single node in Galera Cluster.

    :param host: hostname of the node.
    :param port: port to connect to.
    :param user: MySQL username to connect to the node.
    :param password: MySQL password.
    """

    def __init__(self, host, port=3306, user='root', password=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    @property
    def wsrep_cluster_state_uuid(self):
        """Provides the current State UUID. This is a unique identifier
        for the current state of the cluster and the sequence of changes
        it undergoes.
        """
        return self._status('wsrep_cluster_state_uuid')

    @property
    def wsrep_cluster_status(self):
        """Status of this cluster component. That is, whether the node is
        part of a ``PRIMARY`` or ``NON_PRIMARY`` component."""
        return self._status('wsrep_cluster_status')

    @property
    def wsrep_local_state(self):
        """Internal Galera Cluster FSM state number."""
        return int(self._status('wsrep_local_state'))

    @property
    def wsrep_cluster_name(self):
        """The logical cluster name for the node."""
        result = self.execute("SELECT @@wsrep_cluster_name")
        return result[0]['@@wsrep_cluster_name']

    def execute(self, query, *args):
        """Execute query in Galera Node.

        :param query: Query to execute.
        :type query: str
        :return: Query result or None if the query is not supposed
            to return result.
        :rtype: dict
        """
        with self._connect() as conn:
            return execute(conn, query, *args)

    @contextmanager
    def _connect(self):
        """Connect to Galera node

        :return: MySQL connection to the Galera node
        :rtype: Connection
        """
        connection = pymysql.connect(  # pylint: disable=duplicate-code
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password,
            cursorclass=DictCursor
        )
        yield connection
        connection.close()

    def _status(self, status_variable):
        """Return value of a variable from SHOW GLOBAL STATUS"""
        result = self.execute('SHOW GLOBAL STATUS LIKE %s', status_variable)
        return result[0]['Value']

    def __eq__(self, other):
        return self.host == other.host and self.port == self.port

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return "%s:%d" % (self.host, self.port)
