from contextlib import contextmanager

import pymysql
from pymysql.cursors import DictCursor
from pymysql.err import OperationalError

from proxysql_tools import log
from proxysql_tools.entities.proxysql import (
    ProxySQLMySQLBackend, ProxySQLMySQLUser, BACKEND_STATUS_OFFLINE_HARD
)

PROXYSQL_CONNECT_TIMEOUT = 20


class ProxySQLManager(object):
    def __init__(self, host, port, user, password, socket=None,
                 reload_runtime=True):
        """Initializes the ProxySQL manager.

        :param host: The ProxySQL host to operate against.
        :type host: str
        :param port: The ProxySQL admin port to connect to.
        :type port: int
        :param user: The ProxySQL admin username.
        :type user: str
        :param password: The ProxySQL admin password.
        :type password: str
        :param reload_runtime: Whether the ProxySQL runtime should be
            reloaded for the changes to take affect.
        :type reload_runtime: bool
        """
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.socket = socket
        self.should_reload_runtime = reload_runtime

    def ping(self):
        """Ping the ProxySQL instance to see if its alive."""
        try:
            with self.get_connection() as proxy_conn:
                with proxy_conn.cursor() as cursor:
                    log.debug('Pinging ProxySQL to check if its alive.')

                    cursor.execute('SELECT 1')
        except OperationalError as e:
            log.error('Failed to connect to ProxySQL admin at %s:%s' %
                      (self.host, self.port))

            raise ProxySQLAdminConnectionError(e.message)

        return True

    def reload_runtime(self):
        """Reload the ProxySQL runtime so that the changes take affect."""
        with self.get_connection() as proxy_conn:
            with proxy_conn.cursor() as cursor:
                log.debug('Reloading changes into ProxySQL runtime.')

                cursor.execute('LOAD MYSQL SERVERS TO RUNTIME')
                cursor.execute('LOAD MYSQL USERS TO RUNTIME')
                cursor.execute('LOAD MYSQL VARIABLES TO RUNTIME')

    def register_backend(self, hostgroup_id, hostname, port):
        """Register a MySQL backend with ProxySQL.

        :param hostgroup_id: The ID of the hostgroup that the MySQL backend
            belongs to.
        :type hostgroup_id: int
        :param hostname: The hostname of the MySQL backend.
        :type hostname: str
        :param port: The port that the MySQL backend listens on.
        :type port: int
        :return: True on success, False otherwise.
        :rtype: bool
        """
        backend = ProxySQLMySQLBackend()
        backend.hostgroup_id = hostgroup_id
        backend.hostname = hostname
        backend.port = port

        with self.get_connection() as proxy_conn:
            log.info('Registering backend %s:%s in hostgroup %s.' %
                     (backend.hostname, backend.port, backend.hostgroup_id))

            return self.insert_or_update_mysql_backend(backend, proxy_conn)

    def deregister_backend(self, hostgroup_id, hostname, port):
        """Deregister a MySQL backend from ProxySQL database.

        :param hostgroup_id: The ID of the hostgroup that the MySQL backend
            belongs to.
        :type hostgroup_id: int
        :param hostname: The hostname of the MySQL backend.
        :type hostname: str
        :param port: The port that the MySQL backend listens on.
        :type port: int
        :return: True on success, False otherwise.
        :rtype: bool
        """
        backend = ProxySQLMySQLBackend()
        backend.hostgroup_id = hostgroup_id
        backend.hostname = hostname
        backend.port = port

        with self.get_connection() as proxy_conn:
            if not self.is_mysql_backend_registered(backend, proxy_conn):
                log.debug('Backend %s:%s is not registered in hostgroup %s' %
                          (backend.hostname, backend.port,
                           backend.hostgroup_id))

                return True

            log.info('Deregistering backend %s:%s in hostgroup %s' %
                     (backend.hostname, backend.port, backend.hostgroup_id))

            return self.update_mysql_backend_status(
                backend.hostgroup_id, backend.hostname, backend.port,
                BACKEND_STATUS_OFFLINE_HARD)

    def update_mysql_backend_status(self, hostgroup_id, hostname, port,
                                    status):
        """Update the status of a MySQL backend.

        :param hostgroup_id: The ID of the hostgroup that the MySQL backend
            belongs to.
        :type hostgroup_id: int
        :param hostname: The hostname of the MySQL backend.
        :type hostname: str
        :param port: The port that the MySQL backend listens on.
        :type port: int
        :param status: MySQL backend status.
        :type status: str
        :return: True on success, False otherwise.
        :rtype: bool
        """
        backend = ProxySQLMySQLBackend()
        backend.hostgroup_id = hostgroup_id
        backend.hostname = hostname
        backend.port = port
        backend.status = status

        with self.get_connection() as proxy_conn:
            if not self.is_mysql_backend_registered(backend, proxy_conn):
                err_msg = ('Backend %s:%s is not registered in hostgroup %s' %
                           (backend.hostname, backend.port,
                            backend.hostgroup_id))

                log.error(err_msg)
                raise ProxySQLMySQLBackendUnregistered(err_msg)

            log.info('Updating backend %s:%s in hostgroup %s status to %s' %
                     (backend.hostname, backend.port, backend.hostgroup_id,
                      backend.status))

            return self.insert_or_update_mysql_backend(backend, proxy_conn)

    def fetch_backends(self, hostgroup_id=None):
        """Fetch a list of the MySQL backends registered with ProxySQL, either
        in a particular hostgroup or in all hostgroups.

        :param hostgroup_id: The ID of the hostgroup that the MySQL backend
            belongs to.
        :type hostgroup_id: int
        :return: A list of ProxySQL backends.
        :rtype: list[ProxySQLMySQLBackend]
        """
        with self.get_connection() as proxy_conn:
            with proxy_conn.cursor() as cursor:
                sql = "SELECT * FROM mysql_servers"
                params = []
                if hostgroup_id is not None:
                    sql += " WHERE hostgroup_id=%s"
                    params.append(hostgroup_id)

                cursor.execute(sql, tuple(params))
                backends_list = [ProxySQLMySQLBackend(row) for row in cursor]

        return backends_list

    def register_mysql_user(self, username, password, default_hostgroup):
        """Register a MySQL user with ProxySQL.

        :param username: The MySQL username.
        :type username: str
        :param password: The MySQL user's password hash.
        :type password: str
        :param default_hostgroup: The ID of the hostgroup that is the
            default for this user.
        :type default_hostgroup: int
        :return: True on success, False otherwise.
        :rtype: bool
        """
        user = ProxySQLMySQLUser()
        user.username = username
        user.password = password
        user.default_hostgroup = default_hostgroup

        with self.get_connection() as proxy_conn:
            if self.is_mysql_user_registered(user, proxy_conn):
                log.debug('User %s already registered with default '
                          'hostgroup %s' % (user.username,
                                            user.default_hostgroup))

                return True

            log.info('Registering user %s with default hostgroup %s' %
                     (user.username, user.default_hostgroup))

            return self.insert_or_update_mysql_user(user, proxy_conn)

    def fetch_mysql_users(self, default_hostgroup_id=None):
        """Fetch a list of MySQL users registered with ProxySQL,
        either with a particular default_hostgroup or all of them.

        :param default_hostgroup_id: The ID of the hostgroup which is the
            default for the user.
        :type default_hostgroup_id: int
        :return: A list of MySQL users registered with ProxySQL.
        :rtype: list[ProxySQLMySQLUser]
        """
        with self.get_connection() as proxy_conn:
            with proxy_conn.cursor() as cursor:
                sql = "SELECT * FROM mysql_users"
                params = []
                if default_hostgroup_id is not None:
                    sql += " WHERE default_hostgroup=%s"
                    params.append(default_hostgroup_id)

                cursor.execute(sql, tuple(params))
                users_list = [ProxySQLMySQLUser(row) for row in cursor]

        return users_list

    def set_var(self, variable_name, variable_value):
        """Update ProxySQL variable with the supplied value.

        :param variable_name: The name of the variable to update.
        :type variable_name: str
        :param variable_value: The updated value of the variable.
        :type variable_value: str
        :return: True on success, False otherwise.
        :rtype: bool
        """
        with self.get_connection() as proxy_conn:
            with proxy_conn.cursor() as cursor:
                cursor.execute('UPDATE global_variables SET '
                               'variable_value=%s WHERE variable_name=%s',
                               (variable_value, variable_name))

                cursor.execute('SAVE MYSQL VARIABLES TO DISK')

                if self.should_reload_runtime:
                    cursor.execute('LOAD MYSQL VARIABLES TO RUNTIME')

        return True

    def get_vars(self):
        """Fetch all the variables from ProxySQL.

        :return: A dict of variables in the form {var_name: var_val}.
        :rtype: dict
        """
        with self.get_connection() as proxy_conn:
            with proxy_conn.cursor() as cursor:
                cursor.execute("SELECT variable_name, variable_value "
                               "FROM global_variables")
                res = {r['variable_name'].lower(): r['variable_value'].lower()
                       for r in cursor.fetchall()}

        return res

    @contextmanager
    def get_connection(self):
        db = None
        try:
            if self.socket is not None:
                db = pymysql.connect(
                    unix_socket=self.socket,
                    user=self.user,
                    passwd=self.password,
                    connect_timeout=PROXYSQL_CONNECT_TIMEOUT,
                    cursorclass=DictCursor
                )
            elif self.port:
                db = pymysql.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    passwd=self.password,
                    connect_timeout=PROXYSQL_CONNECT_TIMEOUT,
                    cursorclass=DictCursor
                )
            else:
                db = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    passwd=self.password,
                    connect_timeout=PROXYSQL_CONNECT_TIMEOUT,
                    cursorclass=DictCursor
                )

            yield db
        finally:
            if db:
                db.close()

    @staticmethod
    def is_mysql_backend_registered(backend, proxy_conn):
        """Check ProxySQL to verify whether a MySQL server is registered
        with it or not.

        :param backend: The MySQL backend server to check for registration.
        :type backend: ProxySQLMySQLBackend
        :param proxy_conn: A connection to ProxySQL.
        :type proxy_conn: Connection
        :return: True on success, False otherwise.
        :rtype: bool
        """
        with proxy_conn.cursor() as cursor:
            sql = ("SELECT COUNT(*) AS cnt from mysql_servers "
                   "WHERE hostgroup_id=%s AND hostname=%s AND port=%s")
            cursor.execute(sql, (backend.hostgroup_id, backend.hostname,
                                 backend.port))
            result = cursor.fetchone()

        return int(result['cnt']) > 0

    @staticmethod
    def is_mysql_user_registered(user, proxy_conn):
        """Check ProxySQL to verify whether a MySQL user is registered
        with it or not.

        :param user: The MySQL user to check for registration.
        :type user: ProxySQLMySQLUser
        :param proxy_conn: A connection to ProxySQL.
        :type proxy_conn: Connection
        :return: True on success, False otherwise.
        :rtype: bool
        """
        with proxy_conn.cursor() as cursor:
            sql = ("SELECT COUNT(*) AS cnt from mysql_users "
                   "WHERE username=%s AND backend=%s")
            cursor.execute(sql, (user.username, user.backend))
            result = cursor.fetchone()

        return int(result['cnt']) > 0

    def insert_or_update_mysql_backend(self, backend, proxy_conn):
        """Update the MySQL backend registered with ProxySQL.

        :param backend: The MySQL backend server.
        :type backend: ProxySQLMySQLBackend
        :param proxy_conn: A connection to ProxySQL.
        :type proxy_conn: Connection
        :return: True on success, False otherwise.
        :rtype: bool
        """
        backend.validate()

        col_expressions = []
        val_expressions = []
        for key, val in backend.to_primitive().iteritems():
            col_expressions.append(str(key))
            val_expressions.append("'%s'" % pymysql.escape_string(str(val)))

        with proxy_conn.cursor() as cursor:
            sql = ("REPLACE INTO mysql_servers(%s) VALUES(%s)" %
                   (', '.join(col_expressions), ', '.join(val_expressions)))

            log.debug('Executing query: %s' % sql)

            cursor.execute(sql)
            cursor.execute('SAVE MYSQL SERVERS TO DISK')

            if self.should_reload_runtime:
                cursor.execute('LOAD MYSQL SERVERS TO RUNTIME')

        return True

    def insert_or_update_mysql_user(self, user, proxy_conn):
        """Update the MySQL backend registered with ProxySQL.

        :param user: The MySQL user that will connect to ProxySQL.
        :type user: ProxySQLMySQLUser
        :param proxy_conn: A connection to ProxySQL.
        :type proxy_conn: Connection
        :return: True on success, False otherwise.
        :rtype: bool
        """
        user.validate()

        col_expressions = []
        val_expressions = []
        for key, val in user.to_primitive().iteritems():
            col_expressions.append(str(key))
            val_expressions.append("'%s'" % pymysql.escape_string(str(val)))

        with proxy_conn.cursor() as cursor:
            sql = ("REPLACE INTO mysql_users(%s) VALUES(%s)" %
                   (', '.join(col_expressions), ', '.join(val_expressions)))

            log.debug('Executing query: %s' % sql)

            cursor.execute(sql)
            cursor.execute('SAVE MYSQL USERS TO DISK')

            if self.should_reload_runtime:
                cursor.execute('LOAD MYSQL USERS TO RUNTIME')

        return True


class ProxySQLMySQLBackendUnregistered(Exception):
    pass


class ProxySQLAdminConnectionError(Exception):
    pass
