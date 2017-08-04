from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound
from proxysql_tools.proxysql.proxysqlbackend import ProxySQLMySQLBackend


class ProxySQLMySQLBackendSet(object):
    """ProxySQLMySQLBackendSet contains set of MySQL backends"""
    def __init__(self):
        self._backend_list = []
        self._set_iterator = 0
        pass

    def __len__(self):
        return len(self._backend_list)

    def __contains__(self, item):
        if isinstance(item, ProxySQLMySQLBackend):
            return item in self._backend_list
        elif isinstance(item, ProxySQLMySQLBackendSet):
            for backend in item:
                if backend not in self:
                    return False
            return True
        else:
            return False

    def __eq__(self, other):
        return other == self._backend_list

    def __ne__(self, other):
        return not self.__eq__(other)

    def __iter__(self):
        return self

    def next(self):
        """Return next Backend"""
        try:
            backend = self._backend_list[self._set_iterator]
            self._set_iterator += 1
            return backend
        except IndexError:
            self._set_iterator = 0
            raise StopIteration()

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._backend_list[key]

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

    def add_set(self, backend_set):
        """
        Add iterable object to list

        :param backend_set: Iterable data of backend for adding
        :type backend_set: ProxySQLMySQLBackendSet
        """
        for backend in backend_set:
            self._backend_list.append(backend)

    def add(self, backend):
        """
        Add backend

        :param backend: Backend
        """
        self._backend_list.append(backend)

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
