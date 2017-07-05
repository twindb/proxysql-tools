"""MySQL user commands"""
from __future__ import print_function
from ConfigParser import NoOptionError

from prettytable import PrettyTable
from proxysql_tools.proxysql.proxysql import ProxySQL


def get_users(cfg):
    """Print list of MySQL users from mysql_users"""

    opts_mapping = {
        'host': 'host',  # pylint: disable=duplicate-code
        'port': 'admin_port',  # pylint: disable=duplicate-code
        'user': 'admin_username',  # pylint: disable=duplicate-code
        'password': 'admin_password'  # pylint: disable=duplicate-code
    }

    args = {}
    for key in opts_mapping:
        try:
            args[key] = cfg.get('proxysql', opts_mapping[key])
        except NoOptionError:
            pass

    users = ProxySQL(**args).get_users()

    table = PrettyTable(['username', 'password', 'active',
                         'use_ssl', 'default_hostgroup', 'default_schema',
                         'schema_locked', 'transaction_persistent',
                         'fast_forward', 'backend',
                         'frontend', 'max_connections'])
    for user in users:
        table.add_row([
            user.user,
            user.password,
            user.active,
            user.use_ssl,
            user.default_hostgroup,
            user.default_schema,
            user.schema_locked,
            user.transaction_persistent,
            user.fast_forward,
            user.backend,
            user.frontend,
            user.max_connections
        ])
    print(table)
