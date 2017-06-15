"""ProxySQL classes"""
from contextlib import contextmanager

import pymysql
from pymysql.cursors import DictCursor

from proxysql_tools import LOG, execute
from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound

PROXYSQL_CONNECT_TIMEOUT = 20


class BackendStatus(object):  # pylint: disable=too-few-public-methods
    """Status of ProxySQL backend"""
    online = 'ONLINE'
    shunned = 'SHUNNED'
    offline_soft = 'OFFLINE_SOFT'
    offline_hard = 'OFFLINE_HARD'


class ProxySQLMySQLBackend(object):  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """ProxySQLMySQLBackend describes record in ProxySQL
    table ``mysql_servers``.

.. code-block:: mysql

    CREATE TABLE mysql_servers (
        hostgroup_id INT NOT NULL DEFAULT 0,
        hostname VARCHAR NOT NULL,
        port INT NOT NULL DEFAULT 3306,
        status VARCHAR CHECK (UPPER(status) IN
            ('ONLINE','SHUNNED','OFFLINE_SOFT', 'OFFLINE_HARD'))
            NOT NULL DEFAULT 'ONLINE',
        weight INT CHECK (weight >= 0) NOT NULL DEFAULT 1,
        compression INT CHECK (compression >=0 AND compression <= 102400)
            NOT NULL DEFAULT 0,
        max_connections INT CHECK (max_connections >=0) NOT NULL DEFAULT 1000,
        max_replication_lag INT CHECK (max_replication_lag >= 0
            AND max_replication_lag <= 126144000) NOT NULL DEFAULT 0,
        use_ssl INT CHECK (use_ssl IN(0,1)) NOT NULL DEFAULT 0,
        max_latency_ms INT UNSIGNED CHECK (max_latency_ms>=0)
            NOT NULL DEFAULT 0,
        comment VARCHAR NOT NULL DEFAULT '',
        PRIMARY KEY (hostgroup_id, hostname, port) )

    """
    def __init__(self, hostname, hostgroup_id=0, port=3306,  # pylint: disable=too-many-arguments
                 status=BackendStatus.online,
                 weight=1, compression=0, max_connections=10000,
                 max_replication_lag=0, use_ssl=False,
                 max_latency_ms=0, comment=None):
        self.hostname = hostname
        self.hostgroup_id = int(hostgroup_id)
        self.port = int(port)
        self.status = status
        self.weight = int(weight)
        self.compression = int(compression)
        self.max_connections = int(max_connections)
        self.max_replication_lag = int(max_replication_lag)
        self.use_ssl = bool(use_ssl)
        self.max_latency_ms = int(max_latency_ms)
        self.comment = comment

    def __eq__(self, other):
        try:
            return self.hostgroup_id == other.hostgroup_id and \
                   self.hostname == other.hostname and \
                   self.port == other.port
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return "%d__%s__%d" % (self.hostgroup_id, self.hostname, self.port)


class ProxySQLMySQLUser(object):  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """ProxySQLMySQLUser describes record in ProxySQL table ``mysql_users``.

.. code-block:: mysql

    CREATE TABLE mysql_users (
        username VARCHAR NOT NULL,
        password VARCHAR,
        active INT CHECK (active IN (0,1)) NOT NULL DEFAULT 1,
        use_ssl INT CHECK (use_ssl IN (0,1)) NOT NULL DEFAULT 0,
        default_hostgroup INT NOT NULL DEFAULT 0,
        default_schema VARCHAR,
        schema_locked INT CHECK (schema_locked IN (0,1)) NOT NULL DEFAULT 0,
        transaction_persistent INT CHECK (transaction_persistent IN (0,1))
            NOT NULL DEFAULT 0,
        fast_forward INT CHECK (fast_forward IN (0,1)) NOT NULL DEFAULT 0,
        backend INT CHECK (backend IN (0,1)) NOT NULL DEFAULT 1,
        frontend INT CHECK (frontend IN (0,1)) NOT NULL DEFAULT 1,
        max_connections INT CHECK (max_connections >=0) NOT NULL DEFAULT 10000,
        PRIMARY KEY (username, backend),
        UNIQUE (username, frontend))


:param user: MySQL username to connect to ProxySQL or Galera node.
:param password: MySQL password.
:param active: Users with active = 0 will be tracked in the database,
    but will be never loaded in the in-memory data structures.
:type active: bool
:param use_ssl: Use SSL to connect to MySQL or not
:type use_ssl: bool
:param default_hostgroup: If there is no matching rule for the queries sent by
    the users, the traffic it generates is sent to the specified hostgroup_.
:param default_schema: The schema to which the connection should change
    by default.
:param schema_locked: not supported yet.
:type schema_locked: bool
:param transaction_persistent: if this is set for the user with which the MySQL
    client is connecting to ProxySQL (thus a "frontend" user - see below),
    transactions started within a hostgroup will remain within that hostgroup
    regardless of any other rules.
:type transaction_persistent: bool
:param fast_forward: If set, it bypasses the query processing layer (rewriting,
    caching) and passes the query directly to the backend server.
:type fast_forward: bool
:param frontend: If True, this (username, password) pair is used for
    authenticating to the ProxySQL instance.
:type frontend: bool
:param backend: If True, this (username, password) pair is used for
    authenticating to the mysqld servers against any hostgroup.
:param max_connections: Maximum number of connection this user
    can create to MySQL node.

.. _hostgroup: http://bit.ly/2rGnT5i
    """
    def __init__(self, user='root', password=None, active=False, use_ssl=False,  # pylint: disable=too-many-arguments
                 default_hostgroup=0, default_schema='information_schema',
                 schema_locked=False, transaction_persistent=False,
                 fast_forward=False, backend=False, frontend=True,
                 max_connections=10000):
        self.user = user
        self.password = password
        self.active = active
        self.use_ssl = use_ssl
        self.default_hostgroup = int(default_hostgroup)
        self.default_schema = default_schema
        self.schema_locked = schema_locked
        self.transaction_persistent = transaction_persistent
        self.fast_forward = fast_forward
        self.backend = backend
        self.frontend = frontend
        self.max_connections = int(max_connections)


class ProxySQL(object):
    """
    ProxySQL describes a single ProxySQL instance.

    :param host: ProxySQL hostname.
    :param port: Port on which ProxySQL listens to admin connections.
    :param user: ProxySQL admin user.
    :param password: Password for ProxySQL admin.
    :param socket: Socket to connect to ProxySQL admin interface.
    """
    def __init__(self, host='localhost', port=3306, user='root',  # pylint: disable=too-many-arguments
                 password=None, socket=None):

        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.socket = socket

    def ping(self):
        """Check health of ProxySQL.

        :return: True if ProxySQL healthy and False otherwise.
        :rtype: bool"""
        try:
            result = self.execute('SELECT 1 AS result')
            return result[0]['result'] == '1'
        except pymysql.err.OperationalError:
            LOG.debug('ProxySQL %s:%d is dead', self.host, self.port)
            return False

    def execute(self, query, *args):
        """Execute query in ProxySQL.

        :param query: Query to execute.
        :type query: str
        :return: Query result or None if the query is not supposed
            to return result
        :rtype: dict
        """
        with self._connect() as conn:
            return execute(conn, query, *args)

    def reload_runtime(self):
        """Reload the ProxySQL runtime configuration."""
        self.execute('LOAD MYSQL SERVERS TO RUNTIME')
        self.execute('LOAD MYSQL USERS TO RUNTIME')
        self.execute('LOAD MYSQL VARIABLES TO RUNTIME')

    def register_backend(self, backend):
        """Register Galera node in ProxySQL

        :param backend: Galera node.
        :type backend: ProxySQLMySQLBackend
        """
        if backend.comment:
            comment = "'%s'" % pymysql.escape_string(backend.comment)
        else:
            comment = 'NULL'

        query = "REPLACE INTO mysql_servers(`hostgroup_id`," \
                " `hostname`, `port`," \
                " `status`, `weight`, `compression`, `max_connections`," \
                " `max_replication_lag`, `use_ssl`, `max_latency_ms`," \
                " `comment`) " \
                "VALUES({hostgroup_id}, '{hostname}', {port}," \
                " '{status}', {weight}, {compression}, {max_connections}," \
                " {max_replication_lag}, {use_ssl}, {max_latency_ms}," \
                " {comment})" \
                "".format(hostgroup_id=int(backend.hostgroup_id),
                          hostname=pymysql.escape_string(backend.hostname),
                          port=int(backend.port),
                          status=pymysql.escape_string(backend.status),
                          weight=int(backend.weight),
                          compression=int(backend.compression),
                          max_connections=int(backend.max_connections),
                          max_replication_lag=int(backend.max_replication_lag),
                          use_ssl=int(backend.use_ssl),
                          max_latency_ms=int(backend.max_latency_ms),
                          comment=comment)
        self.execute(query)
        self.reload_runtime()

    def deregister_backend(self, backend):
        """
        Deregister a Galera node from ProxySQL

        :param backend: Galera node.
        :type backend: ProxySQLMySQLBackend
        """
        query = "DELETE FROM mysql_servers WHERE hostgroup_id={hostgroup_id}" \
                " AND hostname='{hostname}'" \
                " AND port={port}" \
                "".format(hostgroup_id=int(backend.hostgroup_id),
                          hostname=pymysql.escape_string(backend.hostname),
                          port=int(backend.port))
        self.execute(query)
        self.reload_runtime()

    def find_backends(self, hostgroup_id, status=None):
        """
        Get writer from mysql_servers

        :param hostgroup_id: writer hostgroup_id
        :type hostgroup_id: int
        :param status: Look only for backends in this status
        :type status: BackendStatus
        :return: Writer MySQL backend or None if doesn't exist
        :rtype: ProxySQLMySQLBackend
        :raise: ProxySQLBackendNotFound
        """
        result = self.execute('SELECT `hostgroup_id`, `hostname`, '
                              '`port`, `status`, `weight`, `compression`, '
                              '`max_connections`, `max_replication_lag`, '
                              '`use_ssl`, `max_latency_ms`, `comment`'
                              ' FROM `mysql_servers`'
                              ' WHERE hostgroup_id = %s', hostgroup_id)

        backends = []
        for row in result:
            backend = ProxySQLMySQLBackend(row['hostname'],
                                           hostgroup_id=row['hostgroup_id'],
                                           port=row['port'],
                                           status=row['status'],
                                           weight=row['weight'],
                                           compression=row['compression'],
                                           max_connections=
                                           row['max_connections'],
                                           max_replication_lag=
                                           row['max_replication_lag'],
                                           use_ssl=row['use_ssl'],
                                           max_latency_ms=
                                           row['max_latency_ms'],
                                           comment=row['comment'])
            if status and backend.status != status:
                continue
            backends.append(backend)
        if backends:
            return backends
        else:
            raise ProxySQLBackendNotFound('Can not find any backends')

    def backend_registered(self, backend):
        """
        Check if backend is registered.

        :param backend: ProxySQLMySQLBackend instance
        :return: True if registered, False otherwise
        :rtype: bool
        """
        result = self.execute('SELECT `hostgroup_id`, `hostname`, '
                              '`port`'
                              ' FROM `mysql_servers`'
                              ' WHERE hostgroup_id = %s '
                              ' AND `hostname` = %s '
                              ' AND `port` = %s',
                              (
                                  backend.hostgroup_id,
                                  backend.hostname,
                                  backend.port
                              ))
        return result != ()

    def set_status(self, backend, status):
        """Update status of a backend in ProxySQL"""
        self.execute('UPDATE `mysql_servers` SET `status` = %s '
                     ' WHERE hostgroup_id = %s '
                     ' AND `hostname` = %s '
                     ' AND `port` = %s',
                     (
                         status,
                         backend.hostgroup_id,
                         backend.hostname,
                         backend.port
                     ))
        self.reload_runtime()

    @contextmanager
    def _connect(self):
        """Connect to ProxySQL admin interface."""
        if self.socket is not None:
            conn = pymysql.connect(unix_socket=self.socket,
                                   user=self.user,
                                   passwd=self.password,
                                   connect_timeout=PROXYSQL_CONNECT_TIMEOUT,
                                   cursorclass=DictCursor)
        else:
            conn = pymysql.connect(host=self.host, port=self.port,
                                   user=self.user, passwd=self.password,
                                   connect_timeout=PROXYSQL_CONNECT_TIMEOUT,
                                   cursorclass=DictCursor)

        yield conn
        conn.close()
