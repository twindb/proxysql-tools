"""Class ProxySQLMySQLBackendSet implementation."""
from proxysql_tools.proxysql.backendset import BackendSet
from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound
from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend


class ProxySQLMySQLBackendSet(BackendSet):
    """ProxySQLMySQLBackendSet contains set of MySQL backends"""

    def __contains__(self, item):
        if isinstance(item, ProxySQLMySQLBackend):
            return item in self._backend_list
        elif isinstance(item, ProxySQLMySQLBackendSet):
            return super(ProxySQLMySQLBackendSet, self).__contains__(item)
        return False

    def find(self, host, hostgroup_id=0, port=3306):
        """
        Find backend by host and port

        :param host: Hostname of backend
        :param hostgroup_id: Hostgroup id of backend
        :param port: Port of backend
        :return: Return found backend
        :rtype: ProxySQLMySQLBackend
        :raises: ProxySQLBackendNotFound
        """
        needle = ProxySQLMySQLBackend(host,
                                      hostgroup_id=hostgroup_id,
                                      port=port)
        for backend in self._backend_list:
            if backend == needle:
                return backend
        raise ProxySQLBackendNotFound('Backend %s not found' % needle)

    def remove(self, backend):
        """Remove backend from the set

        :param backend: Backend to remove.
        :type backend: ProxySQLMySQLBackend
        :raise ProxySQLBackendNotFound: if backend is not in the set
        """
        try:
            self._backend_list.remove(backend)
        except ValueError as err:
            raise ProxySQLBackendNotFound(err)
