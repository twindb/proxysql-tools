import pymysql

from contextlib import contextmanager
from pymysql.cursors import DictCursor


class GaleraManager(object):
    def __init__(self, host, port, user, password, socket=None):
        """Initializes the Galera manager.

        :param str host: The Galera cluster host to operate against.
        :param int port: The MySQL port on Galera cluster host to connect to.
        :param str user: The MySQL username.
        :param str password: The MySQL password.
        """
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.socket = socket

    @contextmanager
    def get_connection(self):
        db = None
        try:
            if self.socket is not None:
                db = pymysql.connect(
                    unix_socket=self.socket,
                    user=self.user,
                    passwd=self.password,
                    cursorclass=DictCursor
                )
            elif self.port:
                db = pymysql.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    passwd=self.password,
                    cursorclass=DictCursor
                )
            else:
                db = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    passwd=self.password,
                    cursorclass=DictCursor
                )

            yield db
        finally:
            if db:
                db.close()
