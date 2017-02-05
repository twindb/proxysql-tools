import pymysql

from contextlib import contextmanager
from pymysql.cursors import DictCursor
from schematics.models import Model
from schematics.types import StringType, IntType

LOCAL_STATE_JOINING = 1
LOCAL_STATE_DONOR_DESYNCED = 2
LOCAL_STATE_JOINED = 3
LOCAL_STATE_SYNCED = 4


class GaleraNode(Model):
    """Contains information about a Galera cluster node."""

    # The hostname or IP address of the cluster node
    host = StringType(required=True)

    # The MySQL port of the cluster node
    port = IntType(required=True)

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
    cluster_status = StringType(required=True, choices=['primary'])

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

        :return bool: Returns True when all properties of the node can be
            refreshed based on its current state.
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

        :return Connection: Returns a MySQL connection to the node.
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