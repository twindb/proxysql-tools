"""MySQL user commands"""
from __future__ import print_function
from ConfigParser import NoOptionError

from prettytable import PrettyTable

from proxysql_tools import OPTIONS_MAPPING
from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLUser


def proxysql_connection_params(cfg):
    """Get ProxySQL connection params from config"""
    args = {}
    for key in OPTIONS_MAPPING:
        try:
            args[key] = cfg.get('proxysql', OPTIONS_MAPPING[key])
        except NoOptionError:
            pass
    return args


def get_users(cfg):
    """Print list of MySQL users from mysql_users"""
    args = proxysql_connection_params(cfg)
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


def create_user(cfg, username, password, active, use_ssl,
                default_hostgroup, default_schema, schema_locked,
                transaction_persistent, fast_forward,
                backend, frontend, max_connections):

    user = ProxySQLMySQLUser(user=username, password=password,
                             active=active,use_ssl=use_ssl,
                             default_hostgroup=default_hostgroup,
                             default_schema=default_schema,
                             schema_locked=schema_locked,
                             transaction_persistent=transaction_persistent,
                             backend=backend, frontend=frontend,
                             fast_forward=fast_forward,
                             max_connections=max_connections)
    args = proxysql_connection_params(cfg)
    ProxySQL(**args).add_user(user)
    pass
