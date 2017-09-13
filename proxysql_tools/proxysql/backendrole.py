"""Module to work with backend roles"""
from json import JSONEncoder, dumps

from .exceptions import ProxySQLError


class BackendRoleEncoder(JSONEncoder):
    """Class encodes BackendRole into json.
    It's used with json.dumps(cls=BackendRoleEncoder)."""
    def default(self, o):  # pylint: disable=method-hidden
        return {
            'writer': o.is_writer(),
            'reader': o.is_reader()
        }


class BackendRole(object):  # pylint: disable=too-few-public-methods
    """ProxySQL backend role"""
    reader = 'Reader'
    writer = 'Writer'

    def __init__(self, writer=False, reader=False):
        self._reader = reader
        self._writer = writer

    def is_writer(self):
        """Returns True if role is a Writer"""
        return self._writer

    def is_reader(self):
        """Returns True if role is a Reader"""
        return self._reader

    @staticmethod
    def roles():
        """
        Returns possible roles

        :return: List with possible BackendRole instances
        :rtype: list(BackendRole)
        """
        result = []
        for writer in [True, False]:
            for reader in [True, False]:
                result.append(BackendRole(writer=writer, reader=reader))
        return result

    def __eq__(self, other):
        """
        Compare this object to other. other can be either instance
        of BackendRole or string "Writer" or "Reader".

        :param other: Other role to compare.
        :type other: BackendRole or str
        :return: True if other is same. False otherwise.
        """
        try:
            return all((
                self.is_writer() == other.is_writer(),
                self.is_reader() == other.is_reader()
            ))
        except AttributeError:
            if other == "Writer":
                return self.is_writer()
            elif other == "Reader":
                return self.is_reader()
            elif other is None:
                return all((
                    not self.is_writer(),
                    not self.is_reader()
                ))
            else:
                raise ProxySQLError('Unexpected role (%s): %r'
                                    % (type(other), other))

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return dumps({
            'writer': self._writer,
            'reader': self._reader
        }, sort_keys=True)
