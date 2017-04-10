import pymysql

from contextlib import contextmanager
from pymysql.cursors import DictCursor
from schematics.models import Model
from schematics.types import StringType, IntType

LOCAL_STATE_JOINING = 1
LOCAL_STATE_DONOR_DESYNCED = 2
LOCAL_STATE_JOINED = 3
LOCAL_STATE_SYNCED = 4

CLUSTER_STATUS_PRIMARY = 'primary'


class GaleraNode(Model):
    """Contains information about a Galera cluster node."""

    # The hostname or IP address of the cluster node
    host = StringType(required=True)

    # The MySQL port of the cluster node
    port = IntType(required=True, default=3306)

    # The name of the cluster this node belongs to
    cluster_name = StringType(required=True)

    # This stores the value of variable `wsrep_cluster_state_uuid` which shows
    # the cluster state UUID which can be used to determine whether the node
    # is part of the cluster. All nodes in the same cluster should have the
    # same UUID
    cluster_state_uuid = StringType(required=True)

    # This stores the value of the variable `wsrep_cluster_status` which shows
    # the primary status of the cluster component that the node is in. The node
    # should only return a value of Primary. Any other value indicates that the
    # node is part of a nonoperational component.
    cluster_status = StringType(required=True,
                                choices=[CLUSTER_STATUS_PRIMARY])

    # Local state of the node in the cluster
    local_state = IntType(required=True, choices=[LOCAL_STATE_JOINING,
                                                  LOCAL_STATE_DONOR_DESYNCED,
                                                  LOCAL_STATE_JOINED,
                                                  LOCAL_STATE_SYNCED])

    # The MySQL username
    username = StringType()

    # The MySQL password
    password = StringType()

    def refresh_state(self):
        """Refreshes the state of the Galera node. Refreshing the state sets the
        appropriate cluster level identifiers corresponding to the cluster
        the node belongs to. It also updates the state of the node in the
        cluster.

        :return: Returns True when all properties of the node can be
            refreshed based on its current state.
        :rtype: bool
        :raises: ModelValidationError, OperationalError
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Fetch the relevant status variables first
                cursor.execute("SHOW GLOBAL STATUS LIKE 'wsrep%'")
                res = {r['Variable_name'].lower(): r['Value'].lower()
                       for r in cursor.fetchall()}

                self.cluster_state_uuid = res.get('wsrep_cluster_state_uuid')
                self.cluster_status = res.get('wsrep_cluster_status')
                self.local_state = int(res.get('wsrep_local_state'))

                # Now fetch the relevant global variables
                cursor.execute('SELECT @@global.wsrep_cluster_name AS '
                               'wsrep_cluster_name')
                res = cursor.fetchone()
                self.cluster_name = res['wsrep_cluster_name']

        self.validate()

        return True

    @contextmanager
    def get_connection(self):
        """Connect to the Galera cluster node.

        :return: Returns a MySQL connection to the node.
        :rtype: Connection
        """
        db = None
        try:
            db = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                passwd=self.password,
                cursorclass=DictCursor
            )

            yield db
        finally:
            if db:
                db.close()

    # To uniquely identify a Galera Node all we need is the host and port
    def __hash__(self):
        return hash('%s__%s' % (self.host, self.port))


class GaleraConfig(Model):
    """A model that defines Galera configuration needed by proxysql-tools."""

    writer_hostgroup_id = StringType(required=True)
    reader_hostgroup_id = StringType(required=True)
    cluster_host = StringType(required=True)
    cluster_username = StringType(required=True)
    cluster_password = StringType(required=True)
    load_balancing_mode = StringType(required=True)
    writer_blacklist = StringType(default=None)
