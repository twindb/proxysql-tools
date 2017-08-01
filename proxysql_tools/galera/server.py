"""MySQL Server commands."""
from __future__ import print_function

from prettytable import PrettyTable

from proxysql_tools import LOG
from proxysql_tools.proxysql.proxysql import ProxySQL, BackendStatus
from proxysql_tools.util import get_proxysql_options, get_backend, \
    get_hostgroups_id


def server_status(cfg):
    """Print list of MySQL servers registered in ProxySQL and their status."""
    kwargs = get_proxysql_options(cfg)
    LOG.debug('ProxySQL config %r', kwargs)
    proxysql = ProxySQL(**kwargs)

    writer_hostgroup_id, reader_hostgroup_id = get_hostgroups_id(cfg)

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


def server_set_wsrep_desync(cfg, server_ip, port, wsrep_desync='ON'):
    """
    Set MySQL server in desync state.

    :param cfg: ProxySQL Tools configuration
    :type cfg: ConfigParser.ConfigParser
    :param server_ip: Server IP address
    :param port: Server port
    :param wsrep_desync: Value for wsrep_desync
    """
    backend = get_backend(cfg, server_ip, port)
    backend.connect(cfg.get('galera', 'cluster_username'),
                    cfg.get('galera', 'cluster_password'))
    backend.execute("SET GLOBAL wsrep_desync=%s", wsrep_desync)


def server_set_admin_status(cfg, server_ip, port, status=BackendStatus.online):
    """
    Set server admin_status
    :param cfg: ProxySQL Tools configuration
    :type cfg: ConfigParser.ConfigParser
    :param server_ip: Server IP address
    :param port: Server port
    :param status: Admin status
    """
    backend = get_backend(cfg, server_ip, port)
    backend.admin_status = status
    kwargs = get_proxysql_options(cfg)
    proxysql = ProxySQL(**kwargs)
    proxysql.set_admin_status(backend)
