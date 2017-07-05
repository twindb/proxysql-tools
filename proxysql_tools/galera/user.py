"""MySQL user commands"""
from ConfigParser import NoOptionError

from prettytable import PrettyTable
from proxysql_tools.proxysql.proxysql import ProxySQL


def get_users(cfg):
    """Print list of MySQL users from mysql_users"""

    option_mapping = {
        'host': 'host',
        'port': 'admin_port',
        'user': 'admin_username',
        'password': 'admin_password'
    }
    args = {}
    for key in option_mapping:
        try:
            args[key] = cfg.get('proxysql', option_mapping[key])
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
