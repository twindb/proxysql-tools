"""MySQL user commands"""
from __future__ import print_function

from prettytable import PrettyTable

from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLUser
from proxysql_tools.util import get_proxysql_options


def get_users(cfg):
    """Print list of MySQL users from mysql_users"""
    args = get_proxysql_options(cfg)
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


def create_user(cfg, kwargs):
    """Create user for MySQL backend"""
    user = ProxySQLMySQLUser(**kwargs)
    args = get_proxysql_options(cfg)
    ProxySQL(**args).add_user(user)


def change_password(cfg, username, password):
    """Change user password"""
    args = get_proxysql_options(cfg)
    user = ProxySQL(**args).get_user(username)
    user.password = password
    ProxySQL(**args).add_user(user)


def delete_user(cfg, username):
    """Delete user from MySQL backend"""
    args = get_proxysql_options(cfg)
    ProxySQL(**args).delete_user(username)
