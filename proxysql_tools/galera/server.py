"""MySQL Server commands."""
from ConfigParser import NoOptionError

from prettytable import PrettyTable

from proxysql_tools import LOG
from proxysql_tools.proxysql.proxysql import ProxySQL, ProxySQLMySQLBackend


def server_status(cfg):
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

    writer_hostgroup_id = int(cfg.get('galera', 'writer_hostgroup_id'))
    reader_hostgroup_id = int(cfg.get('galera', 'reader_hostgroup_id'))

    for hostgroup_id, name in [(writer_hostgroup_id, 'Writers'),
                               (reader_hostgroup_id , 'Readers')]:
        servers = PrettyTable(['hostgroup_id', 'hostname',
                               'port', 'status',
                               'weight', 'compression', 'max_connections',
                               'max_replication_lag', 'use_ssl',
                               'max_latency_ms',
                               'comment'])
        servers.align = 'r'
        servers.align['hostname'] = 'l'
        servers.align['comment'] = 'l'
        LOG.info('%s:' % name)
        for backend in proxysql.find_backends(hostgroup_id):
            servers.add_row([backend.hostgroup_id,
                             backend.hostname,
                             backend.port,
                             backend.status,
                             backend.weight,
                             backend.compression,
                             backend.max_connections,
                             backend.max_replication_lag,
                             backend.use_ssl,
                             backend.max_latency_ms,
                             backend.comment])

        print(servers)
