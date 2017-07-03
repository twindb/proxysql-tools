"""MySQL Server commands."""
from __future__ import print_function

from prettytable import PrettyTable

from proxysql_tools import LOG
from proxysql_tools.proxysql.proxysql import ProxySQL
from proxysql_tools.util import get_proxysql_options


def server_status(cfg):
    """Print list of MySQL servers registered in ProxySQL and their status."""
    kwargs = get_proxysql_options(cfg)
    LOG.debug('ProxySQL config %r', kwargs)
    proxysql = ProxySQL(**kwargs)

    writer_hostgroup_id = int(cfg.get('galera', 'writer_hostgroup_id'))
    reader_hostgroup_id = int(cfg.get('galera', 'reader_hostgroup_id'))

    for hostgroup_id, name in [(writer_hostgroup_id, 'Writers'),
                               (reader_hostgroup_id, 'Readers')]:
        servers = PrettyTable(['hostgroup_id', 'hostname',
                               'port', 'status',
                               'weight', 'compression', 'max_connections',
                               'max_replication_lag', 'use_ssl',
                               'max_latency_ms',
                               'comment'])
        servers.align = 'r'
        servers.align['hostname'] = 'l'  # pylint: disable=unsupported-assignment-operation
        servers.align['comment'] = 'l'   # pylint: disable=unsupported-assignment-operation
        LOG.info('%s:', name)
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
