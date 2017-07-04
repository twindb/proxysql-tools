"""Auxiliary functions."""
from ConfigParser import NoOptionError


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
