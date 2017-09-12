"""Classes to work with MySQL backends."""
import json

import pymysql
from pymysql.cursors import DictCursor

from .backendrole import BackendRole, BackendRoleEncoder
from proxysql_tools import execute


class BackendStatus(object):  # pylint: disable=too-few-public-methods
    """Status of ProxySQL backend"""
    online = 'ONLINE'
    shunned = 'SHUNNED'
    offline_soft = 'OFFLINE_SOFT'
    offline_hard = 'OFFLINE_HARD'


# noinspection LongLine
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
        self.use_ssl = bool(int(use_ssl))
        self.max_latency_ms = int(max_latency_ms)
        self._connection = None
        self.comment = comment
        self._admin_status = None
        try:
            if comment == 'Writer':
                self.role = BackendRole(writer=True)
            elif comment == 'Reader':
                self.role = BackendRole(reader=True)
            else:
                self.role = json.loads(comment)['role']
                if not self.role:
                    self.role = BackendRole()
        except (TypeError, KeyError, ValueError):
            self.role = BackendRole()

        try:
            if comment == 'Writer':
                self._admin_status = status
            elif comment == 'Reader':
                self._admin_status = status
            else:
                self.admin_status = json.loads(comment)['admin_status']
                if not self.admin_status:
                    self.admin_status = None
        except (TypeError, KeyError, ValueError):
            self._admin_status = None

    def __eq__(self, other):
        try:
            return all(
                (
                    self.hostgroup_id == other.hostgroup_id,
                    self.hostname == other.hostname,
                    self.port == other.port
                )
            )
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return json.dumps(self.__dict__, cls=BackendRoleEncoder,
                          sort_keys=True)

    # def __str__(self):
    #     kwargs = {
    #         'hostgroup_id': self.hostgroup_id,
    #         'hostname': self.hostname,
    #         'port': self.port,
    #         'status': self.status,
    #         'weight': self.weight,
    #         'compression': self.compression,
    #         'max_connections': self.max_connections,
    #         'max_replication_lag': self.max_replication_lag,
    #         'use_ssl': self.use_ssl,
    #         'max_latency_ms': self.max_latency_ms,
    #         'comment': self.comment,
    #         'role': self.role
    #     }
    #     return "hostgroup_id={hostgroup_id}, " \
    #            "hostname={hostname}, " \
    #            "port={port}, " \
    #            "role={role}, " \
    #            "status={status}, " \
    #            "weight={weight}, " \
    #            "compression={compression}, " \
    #            "max_connections={max_connections}, " \
    #            "max_replication_lag={max_replication_lag}, " \
    #            "use_ssl={use_ssl}, " \
    #            "max_latency_ms={max_latency_ms}, " \
    #            "comment={comment}".format(**kwargs)

    def _get_admin_status(self):
        return self._admin_status

    def _set_admin_status(self, admin_status):
        self._admin_status = admin_status
        if admin_status:
            self.status = admin_status

    def _del_admin_status(self):
        raise NotImplementedError

    admin_status = property(_get_admin_status,
                            _set_admin_status,
                            _del_admin_status,
                            'Admin status of backend')

    def connect(self, username, password):
        """
        Make a MySQL connection to the backend.

        :param username: MySQL user.
        :param password: MySQL password.
        """
        connection_args = {
            'host': self.hostname,
            'port': self.port,
            'user': username,
            'passwd': password,
            'cursorclass': DictCursor
        }
        self._connection = pymysql.connect(**connection_args)

    def execute(self, query, *args):
        """Execute query in MySQL Backend.

        :param query: Query to execute.
        :type query: str
        :return: Query result or None if the query is not supposed
            to return result
        :rtype: dict
        """
        return execute(self._connection, query, *args)
