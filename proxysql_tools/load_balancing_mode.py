"""Module implements load balancing algorithms"""
from pymysql import OperationalError

from proxysql_tools import LOG
from proxysql_tools.galera.exceptions import GaleraClusterSyncedNodeNotFound, \
    GaleraClusterNodeNotFound
from proxysql_tools.galera.galera_node import GaleraNodeState, GaleraNode
from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound
from proxysql_tools.proxysql.proxysql import ProxySQLMySQLBackend, BackendStatus, \
    BackendRole, ProxySQLMySQLBackendSet


def singlewriter(galera_cluster, proxysql,
                 writer_hostgroup_id,
                 reader_hostgroup_id,
                 ignore_writer=None):
    """
    Implements singlewriter balancing mode.

    :param galera_cluster: GaleraCluster instance.
    :type galera_cluster: GaleraCluster
    :param proxysql: ProxySQL instance
    :type proxysql: ProxySQL
    :param writer_hostgroup_id: Writer hostgroup_id
    :type writer_hostgroup_id: int
    :param reader_hostgroup_id: Reader hostgroup_id
    :type reader_hostgroup_id: int
    :param ignore_writer: Do not make this backend writer
    :type ignore_writer: ProxySQLMySQLBackend
    """
    register_writer(galera_cluster, proxysql, writer_hostgroup_id,
                    reader_hostgroup_id,
                    ignore_writer=ignore_writer)
    register_readers(galera_cluster, proxysql, writer_hostgroup_id,
                     reader_hostgroup_id)

    LOG.debug('Register all missing backends')
    for galera_node in galera_cluster.find_synced_nodes():
        reader = ProxySQLMySQLBackend(galera_node.host,
                                      hostgroup_id=reader_hostgroup_id,
                                      port=galera_node.port,
                                      role=BackendRole.reader)
        writer = ProxySQLMySQLBackend(galera_node.host,
                                      hostgroup_id=writer_hostgroup_id,
                                      port=galera_node.port,
                                      role=BackendRole.writer)
        if not (proxysql.backend_registered(reader) or
                proxysql.backend_registered(writer)):
            proxysql.register_backend(reader)
            LOG.info('Added backend %s to hostgroup %d', reader,
                     reader_hostgroup_id)

    LOG.debug('Make sure writer is not reader')
    writer = proxysql.find_backends(writer_hostgroup_id)[0]
    readers = proxysql.find_backends(reader_hostgroup_id)
    writer_as_reader = writer
    writer_as_reader.hostgroup_id = reader_hostgroup_id

    is_readers_offline = False
    if writer_as_reader in readers:
        readers_without_writer = readers[:]
        readers_without_writer.remove(writer_as_reader)

        is_readers_offline = all(x.status == BackendStatus.offline_soft
                                 for x in readers_without_writer)

    if len(readers) > 2 and proxysql.backend_registered(writer_as_reader) \
        and not is_readers_offline:
        proxysql.deregister_backend(writer_as_reader)


def register_writer(galera_cluster, proxysql, writer_hostgroup_id,
                    reader_hostgroup_id,
                    ignore_writer=None):
    """
    Checks ProxySQL and Galera cluster and makes sure there is one
    healthy registered writer.

    :param galera_cluster: GaleraCluster instance.
    :type galera_cluster: GaleraCluster
    :param proxysql: ProxySQL instance
    :type proxysql: ProxySQL
    :param writer_hostgroup_id: Writer hostgroup_id
    :type writer_hostgroup_id: int
    :param reader_hostgroup_id: Reader hostgroup_id
    :type reader_hostgroup_id: int
    :param ignore_writer: Do not make this backend writer
    :type ignore_writer: ProxySQLMySQLBackend
    """
    # Get writer backend. If doesn't exist - add.
    # If exists - check state of respective Galera node.
    # if the Galera node is not healthy - delete the backend and add a healthy
    # one.
    LOG.debug('Registering writers')
    try:
        for backend in proxysql.find_backends(writer_hostgroup_id):
            check_backend(backend, galera_cluster, proxysql,
                          writer_hostgroup_id, role=BackendRole.writer, limit=1,
                          ignore_backend=ignore_writer,
                          recovered_hostgroup_id=reader_hostgroup_id,
                          recoverd_role=BackendRole.reader)

    except ProxySQLBackendNotFound:
        # add it
        register_synced_backends(galera_cluster, proxysql,
                                 writer_hostgroup_id,
                                 role=BackendRole.writer,
                                 limit=1,
                                 ignore_backend=ignore_writer)

    try:
        proxysql.find_backends(writer_hostgroup_id, BackendStatus.online)
    except ProxySQLBackendNotFound:
        LOG.warn('No writer backends were registered. '
                 'Will try to add previously ignored backends')
        register_synced_backends(galera_cluster, proxysql,
                                 writer_hostgroup_id,
                                role=BackendRole.writer,
                                limit=1)



def register_readers(galera_cluster, proxysql,
                     writer_hostgroup_id, reader_hostgroup_id,
                     ignore_writer=None):
    """
    Checks ProxySQL and Galera cluster and makes sure readers are registered.

    :param galera_cluster: GaleraCluster instance.
    :type galera_cluster: GaleraCluster
    :param proxysql: ProxySQL instance
    :type proxysql: ProxySQL
    :param writer_hostgroup_id: Writer hostgroup_id
    :type writer_hostgroup_id: int
    :param reader_hostgroup_id: Reader hostgroup_id
    :type reader_hostgroup_id: int
    :param ignore_writer: Do not make this backend writer
    :type ignore_writer: ProxySQLMySQLBackend
    """
    LOG.debug('Registering readers')
    try:
        writer = proxysql.find_backends(writer_hostgroup_id)[0]
    except ProxySQLBackendNotFound as err:
        LOG.warn(err)
        register_synced_backends(galera_cluster, proxysql,
                                 writer_hostgroup_id,
                                 role=BackendRole.writer,
                                 limit=1,
                                 ignore_backend=ignore_writer)
        writer = proxysql.find_backends(writer_hostgroup_id)[0]

    try:
        num_readers = 0
        readers = proxysql.find_backends(reader_hostgroup_id)
        for backend in readers:
            LOG.debug('Comparing %s and %s', backend, writer)
            if backend == writer:
                # Don't register writer as reader for now
                continue
            LOG.debug('Do not match')

            check_backend(backend, galera_cluster, proxysql,
                          reader_hostgroup_id, role=BackendRole.reader)
            num_readers += 1
        if num_readers == 0:
            LOG.warn('Did not register any readers')
            check_backend(writer, galera_cluster, proxysql,
                          reader_hostgroup_id, role=BackendRole.reader)

    except ProxySQLBackendNotFound:
        # If there are no readers , register writer as reader as well
        LOG.debug('Reader backends not found')
        writer_as_reader = writer
        writer_as_reader.hostgroup_id = reader_hostgroup_id
        register_synced_backends(galera_cluster, proxysql,
                                 reader_hostgroup_id,
                                 role=BackendRole.reader,
                                 ignore_backend=writer_as_reader)


def check_backend(backend, galera_cluster, proxysql, hostgroup_id, role,  # pylint: disable=too-many-arguments
                  limit=None, ignore_backend=None,
                  recovered_hostgroup_id=None, recoverd_role=None):
    """
    Check health of given backed and if necessary replace it.

    :param backend: MySQL backend.
    :type backend: ProxySQLMySQLBackend
    :param galera_cluster: GaleraCluster instance.
    :type galera_cluster: GaleraCluster
    :param proxysql: ProxySQL instance.
    :type proxysql: ProxySQL
    :param hostgroup_id: Hostgroup_id which the backend belongs to.
    :type hostgroup_id: int
    :param role: Comment to add to mysql_servers in ProxySQL
    :param limit: Register not more than limit number of backends
    :type limit: int
    :param ignore_backend: Do not register this backend
    :type ignore_backend: ProxySQLMySQLBackend
    :param recovered_hostgroup_id: If backend recovers from OFFLINE_SOFT assign
        it to this hostgroup_id. Default hostgroup_id.
    :type recovered_hostgroup_id: int
    :param recoverd_role: If backend recovers from OFFLINE_SOFT set
        this comment
    :type recoverd_role: str
    :return: True if backend successfully registered.
    :rtype: bool
    """
    # check it
    LOG.debug('Backend %s is already registered', backend)
    LOG.debug('Checking its health')
    try:
        node = galera_cluster.find_node(backend.hostname, backend.port)


        state = node.wsrep_local_state
        LOG.debug('%s state: %d', node, state)

        if state == GaleraNodeState.SYNCED:
            LOG.debug('Node %s (%s) is healthy', node, backend.status)

            if backend.admin_status and \
                    backend.admin_status == BackendStatus.offline_hard:
                backend.status = backend.admin_status
                proxysql.register_backend(backend)
                return

            if backend.status != BackendStatus.online:

                LOG.debug('Deregistering %s (%s)', backend, backend.status)
                proxysql.deregister_backend(backend)

                backend.status = BackendStatus.online

                if not recovered_hostgroup_id:
                    recovered_hostgroup_id = hostgroup_id
                backend.hostgroup_id = recovered_hostgroup_id

                if recoverd_role:
                    backend.comment = recoverd_role

                LOG.debug('Registering %s (%s)', backend, backend.status)
                proxysql.register_backend(backend)
        else:
            LOG.warn('Node %s is reachable but unhealty, '
                     'setting it OFFLINE_SOFT', node)
            backend.status = backend.admin_status = BackendStatus.offline_soft
            proxysql.set_admin_status(backend)
            register_synced_backends(galera_cluster, proxysql,
                                     hostgroup_id,
                                     role=role,
                                     limit=limit,
                                     ignore_backend=ignore_backend)

    except GaleraClusterNodeNotFound:
        LOG.warn('Backend %s is not a cluster member. Deregistering it.',
                 backend)
        proxysql.deregister_backend(backend)
        register_synced_backends(galera_cluster, proxysql,
                                 hostgroup_id, role=role,
                                 limit=limit, ignore_backend=ignore_backend)
    except OperationalError as err:
        LOG.error(err)
        LOG.error('Looks like backend %s is unhealthy. Set OFFLINE_HARD status.',
                  backend)
        backend.status = backend.admin_status = BackendStatus.offline_hard
        proxysql.set_admin_status(backend)
        register_synced_backends(galera_cluster, proxysql,
                                 hostgroup_id, role=role,
                                 limit=limit, ignore_backend=ignore_backend)
    return True


def register_synced_backends(galera_cluster, proxysql,  # pylint: disable=too-many-arguments
                             hostgroup_id, role=None, limit=None,
                             ignore_backend=None):
    """
    Find SYNCED node and register it as a backend.

    :param galera_cluster: GaleraCluster instance.
    :type galera_cluster: GaleraCluster
    :param proxysql: ProxySQL instance
    :type proxysql: ProxySQL
    :param hostgroup_id: hostgroup_id
    :type hostgroup_id: int
    :param role: Optional comment to add to mysql_server
    :type role: str
    :param limit: Register not more than limit number of backends
    :type limit: int
    :param ignore_backend: Do not register this backend
    :type ignore_backend: ProxySQLMySQLBackend
    """
    try:
        galera_nodes = galera_cluster.find_synced_nodes()

        if ignore_backend:
            node = GaleraNode(ignore_backend.hostname,
                              port=ignore_backend.port)
            LOG.debug('Ignoring backend %s', ignore_backend)
            if node in galera_nodes:
                LOG.debug('Remove %s from candidates', ignore_backend)
                galera_nodes.remove(node)

        if limit:
            candidate_nodes = galera_nodes[:limit]
        else:
            candidate_nodes = galera_nodes

        for galera_node in candidate_nodes:

            try:
                backends = ProxySQLMySQLBackendSet()
                backends.add_set(proxysql.find_backends(
                    status=BackendStatus.offline_hard,
                    hostgroup_id=hostgroup_id))
                backends.find(galera_node.host, galera_node.port)
                continue
            except ProxySQLBackendNotFound:
                pass

            backend = ProxySQLMySQLBackend(galera_node.host,
                                           hostgroup_id=hostgroup_id,
                                           port=galera_node.port,
                                           role=role)
            proxysql.register_backend(backend)
            LOG.info('Added backend %s to hostgroup %d', backend, hostgroup_id)

    except GaleraClusterSyncedNodeNotFound as err:
        LOG.error(err)
