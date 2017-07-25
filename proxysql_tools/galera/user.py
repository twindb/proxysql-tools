"""MySQL user commands"""
from __future__ import print_function

from prettytable import PrettyTable

from proxysql_tools.util import get_proxysql_options
from proxysql_tools import LOG
from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLUser, BackendStatus
from proxysql_tools.util import parse_user_arguments


def get_encrypred_password(cfg, pwd):
    """Encrypt password with MySQL function PASSWORD()."""
    args = get_proxysql_options(cfg)
    proxysql = ProxySQL(**args)
    writer_hostgroup_id = int(cfg.get('galera', 'writer_hostgroup_id'))

    backends = proxysql.find_backends(writer_hostgroup_id,
                                      BackendStatus.online)
    cluster_username = cfg.get('galera', 'cluster_username')
    cluster_pwd = cfg.get('galera', 'cluster_password')
    backends[0].connect(cluster_username, cluster_pwd)
    result = backends[0].execute('SELECT password(%s);', (
        pwd))
    return result[0].values()[0]


def get_users(cfg):
    """Print list of MySQL users from mysql_users"""
    args = get_proxysql_options(cfg)
    users = ProxySQL(**args).get_users()
    if not users:
        LOG.info('User list is empty')
        return
    table = PrettyTable(['username', 'password', 'active',
                         'use_ssl', 'default_hostgroup', 'default_schema',
                         'schema_locked', 'transaction_persistent',
                         'fast_forward', 'backend',
                         'frontend', 'max_connections'])
    for user in users:
        table.add_row([
            user.username,
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
    args = get_proxysql_options(cfg)
    if kwargs['password']:
        kwargs['password'] = get_encrypred_password(cfg,
                                                    kwargs['password'])
    user = ProxySQLMySQLUser(**kwargs)
    ProxySQL(**args).add_user(user)


def change_password(cfg, username, password):
    """Change user password"""
    password = get_encrypred_password(cfg,
                                      password)
    args = get_proxysql_options(cfg)
    user = ProxySQL(**args).get_user(username)
    user.password = password
    ProxySQL(**args).add_user(user)


def delete_user(cfg, username):
    """Delete user from MySQL backend"""
    args = get_proxysql_options(cfg)
    ProxySQL(**args).delete_user(username)


def modify_user(cfg, username, user_args):
    """Modify user from MySQL backend"""

    args = get_proxysql_options(cfg)
    proxysql = ProxySQL(**args)
    user = proxysql.get_user(username)
    params = parse_user_arguments(user_args)
    for key, value in params.iteritems():
        user.__setattr__(key, value)
    proxysql.add_user(user)
