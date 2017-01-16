import pymysql

from contextlib import contextmanager
from proxysql_tools.entities.proxysql import ProxySQLMySQLBackend, ProxySQLMySQLUser
from pymysql.connections import Connection
from pymysql.cursors import DictCursor


class ProxySQLManager(object):
    def __init__(self, host, port, user, password):
        """Initializes the ProxySQL manager.

        :param str host: The ProxySQL host to operate against.
        :param int port: The ProxySQL admin port to connect to.
        :param str user: The ProxySQL admin username.
        :param str password: The ProxySQL admin password.
        """
        self._host = host
        self._port = port
        self._user = user
        self._password = password

    def register_mysql_backend(self, hostgroup_id, hostname, port):
        """Register a MySQL backend with ProxySQL.

        :param int hostgroup_id: The ID of the hostgroup that the MySQL backend belongs to.
        :param str hostname: The hostname of the MySQL backend.
        :param int port: The port that the MySQL backend listens on.
        :return bool: True on success, False otherwise.
        """
        backend = ProxySQLMySQLBackend()
        backend.hostgroup_id = hostgroup_id
        backend.hostname = hostname
        backend.port = port

        with self.get_connection(self._host, self._port, '', self._user, self._password) as proxy_conn:
            if self.is_mysql_backend_registered(backend, proxy_conn):
                return True

            return self.insert_or_update_mysql_backend(backend, proxy_conn)

    def update_mysql_backend_status(self, hostgroup_id, hostname, port, status):
        """Update the status of a MySQL backend.

        :param int hostgroup_id: The ID of the hostgroup that the MySQL backend belongs to.
        :param str hostname: The hostname of the MySQL backend.
        :param int port: The port that the MySQL backend listens on.
        :param str status: MySQL backend status
        :return bool: True on success, False otherwise.
        """
        backend = ProxySQLMySQLBackend()
        backend.hostgroup_id = hostgroup_id
        backend.hostname = hostname
        backend.port = port
        backend.status = status

        with self.get_connection(self._host, self._port, '', self._user, self._password) as proxy_conn:
            if not self.is_mysql_backend_registered(backend, proxy_conn):
                raise ProxySQLMySQLBackendUnregistered('MySQL backend %s:%s is not registered' %
                                                       (backend.hostname, backend.port))

            return self.insert_or_update_mysql_backend(backend, proxy_conn)

    def register_mysql_users(self):
        pass

    def register_single_mysql_user(self):
        pass

    def unregister_mysql_backend(self):
        pass

    def set_variables(self):
        pass

    def fetch_mysql_users(self):
        pass

    def fetch_registered_mysql_users(self):
        pass

    @contextmanager
    def get_connection(self, host, port, mysql_sock, user, password):
        db = None
        try:
            if mysql_sock != '':
                db = pymysql.connect(
                    unix_socket=mysql_sock,
                    user=user,
                    passwd=password,
                    cursorclass=DictCursor
                )
            elif port:
                db = pymysql.connect(
                    host=host,
                    port=int(port),
                    user=user,
                    passwd=password,
                    cursorclass=DictCursor
                )
            else:
                db = pymysql.connect(
                    host=host,
                    user=user,
                    passwd=password,
                    cursorclass=DictCursor
                )

            yield db
        finally:
            if db:
                db.close()

    @staticmethod
    def is_mysql_backend_registered(backend, proxy_conn):
        """Check ProxySQL to verify whether a MySQL server is registered with it or not.

        :param ProxySQLMySQLBackend backend: The MySQL backend server to check for registration.
        :param Connection proxy_conn: A connection to ProxySQL.
        :return bool: True on success, False otherwise.
        """
        with proxy_conn.cursor() as cursor:
            sql = "SELECT COUNT(*) AS cnt from mysql_servers WHERE hostgroup_id=%s AND hostname=%s AND port=%s"
            cursor.execute(sql, (backend.hostgroup_id, backend.hostname, backend.port))
            result = cursor.fetchone()

        return int(result['cnt']) > 0

    @staticmethod
    def insert_or_update_mysql_backend(backend, proxy_conn):
        """Update the MySQL backend registered with ProxySQL.

        :param ProxySQLMySQLBackend backend: The MySQL backend server.
        :param Connection proxy_conn: A connection to ProxySQL.
        :return bool: True on success, False otherwise.
        """
        backend.validate()

        column_expressions = []
        value_expressions = []
        for key, val in backend.to_primitive().iteritems():
            column_expressions.append(str(key))
            value_expressions.append("'%s'" % pymysql.escape_string(str(val)))

        with proxy_conn.cursor() as cursor:
            sql = "REPLACE INTO mysql_servers(%s) VALUES(%s)" % (', '.join(column_expressions),
                                                                 ', '.join(value_expressions))
            cursor.execute(sql)
            cursor.execute('LOAD MYSQL SERVERS TO RUNTIME')
            cursor.execute('SAVE MYSQL SERVERS TO DISK')

        return True


class ProxySQLMySQLBackendUnregistered(Exception):
    pass
