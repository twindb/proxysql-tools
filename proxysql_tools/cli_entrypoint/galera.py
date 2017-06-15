"""Galera entrypoints"""
from ConfigParser import NoOptionError

from proxysql_tools import LOG
from proxysql_tools.galera.galera_cluster import GaleraCluster
from proxysql_tools.load_balancing_mode import singlewriter
from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLBackend


def galera_register(cfg):
    """Registers Galera cluster nodes with ProxySQL."""

    kwargs = {}
    try:
        kwargs['user'] = cfg.get('galera', 'cluster_username')
    except NoOptionError:
        pass
    try:
        kwargs['password'] = cfg.get('galera', 'cluster_password')
    except NoOptionError:
        pass

    LOG.debug('Galera config %r', kwargs)
    galera_cluster = GaleraCluster(cfg.get('galera', 'cluster_host'),
                                   **kwargs)

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
    LOG.debug('ProxySQL config %r', kwargs)
    proxysql = ProxySQL(**kwargs)

    load_balancing_mode = cfg.get('galera', 'load_balancing_mode')
    writer_hostgroup_id = int(cfg.get('galera', 'writer_hostgroup_id'))
    reader_hostgroup_id = int(cfg.get('galera', 'reader_hostgroup_id'))

    if load_balancing_mode == 'singlewriter':
        kwargs = {}
        try:
            host, port = cfg.get('galera', 'writer_blacklist').split(':')
            bcknd = ProxySQLMySQLBackend(host,
                                         hostgroup_id=writer_hostgroup_id,
                                         port=port)
            kwargs['ignore_writer'] = bcknd
        except NoOptionError:
            pass
        singlewriter(galera_cluster, proxysql,
                     writer_hostgroup_id, reader_hostgroup_id, **kwargs)
    else:
        raise NotImplementedError('Balancing mode %s not implemented yet.'
                                  % load_balancing_mode)
