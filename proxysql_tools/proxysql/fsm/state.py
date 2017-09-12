"""State class"""
from copy import deepcopy
from itertools import product

from ..proxysqlbackendset import ProxySQLMySQLBackendSet
from ..backendrole import BackendRole


class ProxySQLState(object):
    def __init__(self, backend_set):
        """
        Create a state for a set of nodes declared in backend_set

        :param backend_set: Backend set
        :type backend_set: ProxySQLMySQLBackendSet
        """
        # print('backends:')
        # print(backend_set)
        self._backends = backend_set
        self._roles = self._roles_map()

    @property
    def backends(self):
        return self._backends

    def _roles_map(self):
        # noinspection LongLine
        """
                Depending on number of backends it returns a list of possible roles.
                 If there is one backend the result will be

.. code-block:: json

    [
        ({"reader": true, "writer": true}, ),
        ({"reader": false, "writer": true}, ),
        ({"reader": true, "writer": false}, ),
        ({"reader": false, "writer": false}, )
    ]

                if there are two backends the result will be

.. code-block:: json

    [
        ({"reader": true, "writer": true}, {"reader": true, "writer": true})
        ({"reader": true, "writer": true}, {"reader": false, "writer": true})
        ({"reader": true, "writer": true}, {"reader": true, "writer": false})
        ({"reader": true, "writer": true}, {"reader": false, "writer": false})
        ({"reader": false, "writer": true}, {"reader": true, "writer": true})
        ({"reader": false, "writer": true}, {"reader": false, "writer": true})
        ({"reader": false, "writer": true}, {"reader": true, "writer": false})
        ({"reader": false, "writer": true}, {"reader": false, "writer": false})
        ({"reader": true, "writer": false}, {"reader": true, "writer": true})
        ({"reader": true, "writer": false}, {"reader": false, "writer": true})
        ({"reader": true, "writer": false}, {"reader": true, "writer": false})
        ({"reader": true, "writer": false}, {"reader": false, "writer": false})
        ({"reader": false, "writer": false}, {"reader": true, "writer": true})
        ({"reader": false, "writer": false}, {"reader": false, "writer": true})
        ({"reader": false, "writer": false}, {"reader": true, "writer": false})
        ({"reader": false, "writer": false}, {"reader": false, "writer": false})
    ]

                And for three nodes the result will be

.. code-block:: json

    [

        ({"reader": true, "writer": true}, {"reader": true, "writer": true}, {"reader": true, "writer": true},),
        ({"reader": true, "writer": true}, {"reader": true, "writer": true}, {"reader": false, "writer": true},),
        ({"reader": true, "writer": true}, {"reader": true, "writer": true}, {"reader": true, "writer": false},),
        ({"reader": true, "writer": true}, {"reader": true, "writer": true}, {"reader": false, "writer": false},),
        ({"reader": true, "writer": true}, {"reader": false, "writer": true}, {"reader": true, "writer": true},),
        ({"reader": true, "writer": true}, {"reader": false, "writer": true}, {"reader": false, "writer": true},),
        ({"reader": true, "writer": true}, {"reader": false, "writer": true}, {"reader": true, "writer": false},),
        ({"reader": true, "writer": true}, {"reader": false, "writer": true}, {"reader": false, "writer": false},),
        ({"reader": true, "writer": true}, {"reader": true, "writer": false}, {"reader": true, "writer": true},),
        ({"reader": true, "writer": true}, {"reader": true, "writer": false}, {"reader": false, "writer": true},),
        ({"reader": true, "writer": true}, {"reader": true, "writer": false}, {"reader": true, "writer": false},),
        ({"reader": true, "writer": true}, {"reader": true, "writer": false}, {"reader": false, "writer": false},),
        ({"reader": true, "writer": true}, {"reader": false, "writer": false}, {"reader": true, "writer": true},),
        ({"reader": true, "writer": true}, {"reader": false, "writer": false}, {"reader": false, "writer": true},),
        ({"reader": true, "writer": true}, {"reader": false, "writer": false}, {"reader": true, "writer": false},),
        ({"reader": true, "writer": true}, {"reader": false, "writer": false}, {"reader": false, "writer": false},),
        ({"reader": false, "writer": true}, {"reader": true, "writer": true}, {"reader": true, "writer": true},),
        ({"reader": false, "writer": true}, {"reader": true, "writer": true}, {"reader": false, "writer": true},),
        ({"reader": false, "writer": true}, {"reader": true, "writer": true}, {"reader": true, "writer": false},),
        ({"reader": false, "writer": true}, {"reader": true, "writer": true}, {"reader": false, "writer": false},),
        ({"reader": false, "writer": true}, {"reader": false, "writer": true}, {"reader": true, "writer": true},),
        ({"reader": false, "writer": true}, {"reader": false, "writer": true}, {"reader": false, "writer": true},),
        ({"reader": false, "writer": true}, {"reader": false, "writer": true}, {"reader": true, "writer": false},),
        ({"reader": false, "writer": true}, {"reader": false, "writer": true}, {"reader": false, "writer": false},),
        ({"reader": false, "writer": true}, {"reader": true, "writer": false}, {"reader": true, "writer": true},),
        ({"reader": false, "writer": true}, {"reader": true, "writer": false}, {"reader": false, "writer": true},),
        ({"reader": false, "writer": true}, {"reader": true, "writer": false}, {"reader": true, "writer": false},),
        ({"reader": false, "writer": true}, {"reader": true, "writer": false}, {"reader": false, "writer": false},),
        ({"reader": false, "writer": true}, {"reader": false, "writer": false}, {"reader": true, "writer": true},),
        ({"reader": false, "writer": true}, {"reader": false, "writer": false}, {"reader": false, "writer": true},),
        ({"reader": false, "writer": true}, {"reader": false, "writer": false}, {"reader": true, "writer": false},),
        ({"reader": false, "writer": true}, {"reader": false, "writer": false}, {"reader": false, "writer": false},),
        ({"reader": true, "writer": false}, {"reader": true, "writer": true}, {"reader": true, "writer": true},),
        ({"reader": true, "writer": false}, {"reader": true, "writer": true}, {"reader": false, "writer": true},),
        ({"reader": true, "writer": false}, {"reader": true, "writer": true}, {"reader": true, "writer": false},),
        ({"reader": true, "writer": false}, {"reader": true, "writer": true}, {"reader": false, "writer": false},),
        ({"reader": true, "writer": false}, {"reader": false, "writer": true}, {"reader": true, "writer": true},),
        ({"reader": true, "writer": false}, {"reader": false, "writer": true}, {"reader": false, "writer": true},),
        ({"reader": true, "writer": false}, {"reader": false, "writer": true}, {"reader": true, "writer": false},),
        ({"reader": true, "writer": false}, {"reader": false, "writer": true}, {"reader": false, "writer": false},),
        ({"reader": true, "writer": false}, {"reader": true, "writer": false}, {"reader": true, "writer": true},),
        ({"reader": true, "writer": false}, {"reader": true, "writer": false}, {"reader": false, "writer": true},),
        ({"reader": true, "writer": false}, {"reader": true, "writer": false}, {"reader": true, "writer": false},),
        ({"reader": true, "writer": false}, {"reader": true, "writer": false}, {"reader": false, "writer": false},),
        ({"reader": true, "writer": false}, {"reader": false, "writer": false}, {"reader": true, "writer": true},),
        ({"reader": true, "writer": false}, {"reader": false, "writer": false}, {"reader": false, "writer": true},),
        ({"reader": true, "writer": false}, {"reader": false, "writer": false}, {"reader": true, "writer": false},),
        ({"reader": true, "writer": false}, {"reader": false, "writer": false}, {"reader": false, "writer": false},),
        ({"reader": false, "writer": false}, {"reader": true, "writer": true}, {"reader": true, "writer": true},),
        ({"reader": false, "writer": false}, {"reader": true, "writer": true}, {"reader": false, "writer": true},),
        ({"reader": false, "writer": false}, {"reader": true, "writer": true}, {"reader": true, "writer": false},),
        ({"reader": false, "writer": false}, {"reader": true, "writer": true}, {"reader": false, "writer": false},),
        ({"reader": false, "writer": false}, {"reader": false, "writer": true}, {"reader": true, "writer": true},),
        ({"reader": false, "writer": false}, {"reader": false, "writer": true}, {"reader": false, "writer": true},),
        ({"reader": false, "writer": false}, {"reader": false, "writer": true}, {"reader": true, "writer": false},),
        ({"reader": false, "writer": false}, {"reader": false, "writer": true}, {"reader": false, "writer": false},),
        ({"reader": false, "writer": false}, {"reader": true, "writer": false}, {"reader": true, "writer": true},),
        ({"reader": false, "writer": false}, {"reader": true, "writer": false}, {"reader": false, "writer": true},),
        ({"reader": false, "writer": false}, {"reader": true, "writer": false}, {"reader": true, "writer": false},),
        ({"reader": false, "writer": false}, {"reader": true, "writer": false}, {"reader": false, "writer": false},),
        ({"reader": false, "writer": false}, {"reader": false, "writer": false}, {"reader": true, "writer": true},),
        ({"reader": false, "writer": false}, {"reader": false, "writer": false}, {"reader": false, "writer": true},),
        ({"reader": false, "writer": false}, {"reader": false, "writer": false}, {"reader": true, "writer": false},),
        ({"reader": false, "writer": false}, {"reader": false, "writer": false}, {"reader": false, "writer": false},),

    ]
                :return: All possible combinations of roles
                :rtype: list(tuple(BackendRole))
                """
        roles = []

        for _ in self._backends:
            if roles:
                roles = product(BackendRole().roles(), roles)
            else:
                cart_product = product(BackendRole().roles(), repeat=1)
                for role in cart_product:
                    roles.append(role)
        result = []
        for role in roles:
            result.append(self._unpack_role(role))

        return result

    def _unpack_role(self, roles_byte):
        # noinspection LongLine
        """Converts roles from form

.. code-block:: json

    ({"reader": true, "writer": false}, ({"reader": false, "writer": false}, ({"reader": false, "writer": false},)))

    to form

.. code-block:: json

    ({"reader": true, "writer": false}, {"reader": false, "writer": false}, {"reader": false, "writer": false},)


        return: tuple with roles. Number of items in the tuple equals to number of backends.
        """
        try:
            return (roles_byte[0],) + self._unpack_role(roles_byte[1])
        except IndexError:
            return roles_byte[0],

    def states(self):

        all_states = []

        for role in self._roles:

            backend_set = ProxySQLMySQLBackendSet()
            i = 0
            for backend in self._backends:

                new_be = deepcopy(backend)
                new_be.role = role[i]

                backend_set.add(new_be)

                i += 1

            all_states.append(backend_set)

        return all_states



    # nodes = {n1, n2, n3}
    # roles = {nr, nw, r, w}
    #
    # nr = 12

