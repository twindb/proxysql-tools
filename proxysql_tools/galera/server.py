"""MySQL Server commands."""
from __future__ import print_function

from prettytable import PrettyTable

from proxysql_tools import LOG
from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound
from proxysql_tools.proxysql.proxysql import ProxySQL
from proxysql_tools.proxysql.proxysqlbackend import BackendStatus
from proxysql_tools.util import get_proxysql_options, get_hostgroups_id


def server_status(cfg):
    """Print list of MySQL servers registered in ProxySQL and their status."""
    kwargs = get_proxysql_options(cfg)
    LOG.debug('ProxySQL config %r', kwargs)
    proxysql = ProxySQL(**kwargs)

    writer_hostgroup_id, reader_hostgroup_id = get_hostgroups_id(cfg)

    for hostgroup_id, name in [(writer_hostgroup_id, 'Writers'),
                               (reader_hostgroup_id, 'Readers')]:
        columns = [
            'hostgroup_id',
            'hostname',
            'port',
            'status',
            'weight',
            'compression',
            'max_connections',
            'max_replication_lag',
            'use_ssl',
            'max_latency_ms',
            'comment'
        ]
        servers = PrettyTable(columns)

        servers.align = 'r'
        servers.align['hostname'] = 'l'  # pylint: disable=unsupported-assignment-operation
        servers.align['comment'] = 'l'   # pylint: disable=unsupported-assignment-operation

        LOG.info('%s:', name)
        try:
            for backend in proxysql.find_backends(hostgroup_id):
                row = [
                    backend.hostgroup_id,
                    backend.hostname,
                    backend.port,
                    backend.status,
                    backend.weight,
                    backend.compression,
                    backend.max_connections,
                    backend.max_replication_lag,
                    backend.use_ssl,
                    backend.max_latency_ms,
                    backend.comment
                ]
                servers.add_row(row)

            print(servers)
        except ProxySQLBackendNotFound as err:
            LOG.warning(err)


def server_set_wsrep_desync(cfg, server_ip, port, wsrep_desync='ON'):
    """
    Set MySQL server in desync state.

    :param cfg: ProxySQL Tools configuration
    :type cfg: ConfigParser.ConfigParser
    :param server_ip: Server IP address
    :param port: Server port
    :param wsrep_desync: Value for wsrep_desync
    """
    kwargs = get_proxysql_options(cfg)
    proxysql = ProxySQL(**kwargs)
    backends = proxysql.find_backends()

    cluster_username = cfg.get('galera', 'cluster_username')
    cluster_password = cfg.get('galera', 'cluster_password')

    for group_id in get_hostgroups_id(cfg):

        try:
            backend = backends.find(server_ip,
                                    port=port,
                                    hostgroup_id=group_id)
            backend.connect(cluster_username, cluster_password)
            backend.execute("SET GLOBAL wsrep_desync=%s", wsrep_desync)
        except ProxySQLBackendNotFound:
            pass


def server_set_admin_status(cfg, server_ip, port,
                            status=BackendStatus.online):
    """
    Set server admin_status

    :param cfg: ProxySQL Tools configuration
    :type cfg: ConfigParser.ConfigParser
    :param server_ip: Server IP address
    :param port: Server port
    :param status: Admin status
    """
    kwargs = get_proxysql_options(cfg)
    proxysql = ProxySQL(**kwargs)
    backends = proxysql.find_backends()

    for group_id in get_hostgroups_id(cfg):

        try:
            backend = backends.find(server_ip,
                                    port=port,
                                    hostgroup_id=group_id)
            backend.admin_status = status
            LOG.debug('Updating %s', backend)
            proxysql.update_backend(backend)
        except ProxySQLBackendNotFound:
            LOG.debug('Skipping hostgroup_id %d', group_id)
