"""State class"""
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
        print('backends:')
        print(backend_set)
        self._backends = backend_set
        # self._roles = BackendRole().roles()

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
        roles = None

        for _ in self._backends:
            if roles:
                roles = product(BackendRole().roles(), roles)
            else:
                roles = product(BackendRole().roles(), repeat=1)

        result = []
        for role in roles:
            print(role)
            result.append(role)
        return result

    def states(self):
        roles = None

        for _ in self._backends:
            if roles:
                roles = product(BackendRole().roles(), roles)
            else:
                roles = product(BackendRole().roles(), repeat=1)

        # print(roles)
        l = 0
        all_states = []
        for role in roles:
            print(role)
            role_unpacked = role[0]
            # print(type(role[1]))

            backend_set = ProxySQLMySQLBackendSet()
            for backend in self._backends:
                # print(role_unpacked)
                if isinstance(role, tuple):
                    backend.role = role[0]
                else:
                    backend.role = role
                backend_set.add(backend)
                try:
                    role = role[1]
                except IndexError:
                    pass

            all_states.append(backend_set)
            # print(backend_set)
            l += 1

        print(len(all_states))
        # roles.
        print(l)
        return roles



    # nodes = {n1, n2, n3}
    # roles = {nr, nw, r, w}
    #
    # nr = 12

