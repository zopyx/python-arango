"""ArangoDB's graph API."""


from arango.api import DatabaseSpecificAPI
from arango.constants import HTTP_OK
from arango.exceptions import (
    GraphListError,
    GraphGetError,
    GraphCreateError,
    GraphDeleteError,
)
from arango.utils import uncamelify


class GraphManagementAPI(DatabaseSpecificAPI):
    """A wrapper around ArangoDB's graph API."""

    def __init__(self, connection):
        super(GraphManagementAPI, self).__init__(connection)

    def list_graphs(self):
        """List all graphs in this database.

        :returns: the graphs in this database
        :rtype: dict
        :raises: GraphGetError
        """
        res = self.conn.api_get("/gharial")
        if res.status_code not in HTTP_OK:
            raise GraphListError(res)
        return [graph["_key"] for graph in res.obj["graphs"]]

    def get_graph(self, name):
        """Return the properties of this graph.

        :returns: the properties of this graph
        :rtype: dict
        :raises: GraphPropertyError
        """
        res = self.conn.api_get(
            "/gharial/{}".format(name)
        )
        if res.status_code not in HTTP_OK:
            raise GraphGetError(res)
        return uncamelify(res.obj["graph"])

    def create_graph(self, name, edge_definitions=None,
                     orphan_collections=None):
        """Add a new graph in this database.

        # TODO expand on edge_definitions and orphan_collections

        :param name: name of the new graph
        :type name: str
        :param edge_definitions: definitions for edges
        :type edge_definitions: list
        :param orphan_collections: names of additional vertex collections
        :type orphan_collections: list
        :returns: the graph object
        :rtype: arango.graph.Graph
        :raises: GraphCreateError
        """
        data = {"name": name}
        if edge_definitions is not None:
            data["edgeDefinitions"] = edge_definitions
        if orphan_collections is not None:
            data["orphanCollections"] = orphan_collections

        res = self.conn.api_post("/gharial", data=data)
        if res.status_code not in HTTP_OK:
            raise GraphCreateError(res)
        return res.obj

    def delete_graph(self, name):
        """Delete the graph of the given name from this database.

        :param name: the name of the graph to delete
        :type name: str
        :raises: GraphDeleteError
        """
        res = self.conn.api_delete("/gharial/{}".format(name))
        if res.status_code not in HTTP_OK:
            raise GraphDeleteError(res)
        return res.obj
