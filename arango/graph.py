"""Wrapper for ArangoDB's graph-specific API including:

- graph properties
- vertex collections
- edge definitions
- vertexes
- edges
- traversals

"""

from arango.utils import uncamelify
from arango.exceptions import *


class Graph(object):
    """A wrapper for ArangoDB graph specific API.

    :param connection: the ArangoDB connection object
    :type connection: arango.connection.ArangoConnection
    :param graph_name: the name of the graph
    :type graph_name: str
    """

    def __init__(self, connection, graph_name):
        self.conn = connection
        self.graph_name = graph_name

    @property
    def properties(self):
        """Return the properties of this graph.

        :returns: the properties of this graph
        :rtype: dict
        :raises: GraphPropertyError
        """
        res = self.conn.api_get(
            "/gharial/{}".format(self.graph_name)
        )
        if res.status_code != 200:
            raise GraphPropertyError(res)
        return uncamelify(res.obj["graph"])

    @property
    def id(self):
        """Return the ID of this graph.

        :returns: the ID of this graph
        :rtype: str
        :raises: GraphPropertyError
        """
        return self.properties["_id"]

    @property
    def revision(self):
        """Return the revision of this graph.

        :returns: the revision of this graph
        :rtype: str
        :raises: GraphPropertyError
        """
        return self.properties["_rev"]

    ######################
    # Vertex Collections #
    ######################

    @property
    def orphan_collections(self):
        """Return the orphan collections of this graph.

        :returns: the string names of the orphan collections
        :rtype: list
        :raises: GraphPropertyError
        """
        return self.properties["orphan_collections"]



    ####################
    # Edge Definitions #
    ####################



    ############
    # Vertexes #
    ############



    #########
    # Edges #
    #########
