"""Auxiliary functions."""
from ConfigParser import NoOptionError

from proxysql_tools.proxysql.proxysql import ProxySQL
from proxysql_tools.proxysql.proxysqlbackendset import ProxySQLMySQLBackendSet


def get_proxysql_options(cfg):
    """Get ProxySQL relevant config options"""
    kwargs = {}
    try:
        kwargs['host'] = cfg.get('proxysql', 'host')
    except NoOptionError:
        pass
    try:
        kwargs['port'] = cfg.get('proxysql', 'admin_port')
    except NoOptionError:
        pass
    try:
        kwargs['user'] = cfg.get('proxysql', 'admin_username')
    except NoOptionError:
        pass
    try:
        kwargs['password'] = cfg.get('proxysql', 'admin_password')
    except NoOptionError:
        pass
    try:
        kwargs['socket'] = cfg.get('proxysql', 'admin_socket')
    except NoOptionError:
        pass

    return kwargs


def get_hostgroups_id(cfg):
    """Get writer and reader hostgroup id's """
    writer_hostgroup_id = int(cfg.get('galera', 'writer_hostgroup_id'))
    reader_hostgroup_id = int(cfg.get('galera', 'reader_hostgroup_id'))
    return writer_hostgroup_id, reader_hostgroup_id


def get_backend(kwargs, server_ip, port):
    """Get backend by server_ip and port"""
    proxysql = ProxySQL(**kwargs)
    backends = ProxySQLMySQLBackendSet()
    backends.add_set(proxysql.find_backends())
    return backends.find(server_ip, port)


def parse_user_arguments(args):
    """Parse user arguments for modify_user"""
    attrs = {
        '--use_ssl': {
            'value': True,
            'key': 'use_ssl'
        },
        '--no-use_ssl': {
            'value': False,
            'key': 'use_ssl'
        },
        '--active': {
            'value': True,
            'key': 'active'
        },
        '--no-active': {
            'value': False,
            'key': 'active'
        },
        '--default_hostgroup': {
            'value': int,
            'key': 'default_hostgroup'
        },
        '--default_schema': {
            'value': str,
            'key': 'default_schema'
        },
        '--schema_locked': {
            'value': True,
            'key': 'schema_locked'
        },
        '--no-schema_locked': {
            'value': False,
            'key': 'schema_locked'
        },
        '--transaction_persistent': {
            'value': True,
            'key': 'transaction_persistent'
        },
        '--no-transaction_persistent': {
            'value': False,
            'key': 'transaction_persistent'
        },
        '--backend': {
            'value': True,
            'key': 'backend'
        },
        '--frontend': {
            'value': True,
            'key': 'frontend'
        },
        '--no-backend': {
            'value': False,
            'key': 'backend'
        },
        '--no-frontend': {
            'value': False,
            'key': 'frontend'
        },
        '--fast_forward': {
            'value': True,
            'key': 'fast_forward'
        },
        '--no-fast_forward': {
            'value': False,
            'key': 'fast_forward'
        },
        '--max_connections': {
            'value': int,
            'key': 'max_connections'
        }
    }
    result = {}
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in ['--max_connections', '--default_hostgroup',
                   '--default_schema']:
            try:
                result[attrs[arg]['key']] = attrs[arg]['value'](args[i + 1])
            except (IndexError, ValueError):
                raise ValueError
            i += 2
            continue
        elif arg in attrs.keys():
            result[attrs[arg]['key']] = attrs[arg]['value']
        else:
            raise ValueError('Unexpected argument: %s', arg)
        i += 1
    return result
