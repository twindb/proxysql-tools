"""Class BackendSet implementation."""

from abc import abstractmethod


class BackendSet(object):
    """Base class for sets of nodes and backends"""

    def __init__(self):
        self._backend_list = []
        self._set_iterator = 0

    def __len__(self):
        return len(self._backend_list)

    def __contains__(self, item):
        for backend in item:
            if backend not in self:
                return False
        return True

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

    def add_set(self, backend_set):
        """
        Add iterable object to list

        :param backend_set:
        :type backend_set:
        """
        for backend in backend_set:
            self._backend_list.append(backend)

    def add(self, backend):
        """
        Add backend

        :param backend: Backend
        """
        self._backend_list.append(backend)

    @abstractmethod
    def remove(self, backend):
        """Remove backend from the set"""
