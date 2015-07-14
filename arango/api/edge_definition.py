"""ArangoDB's edge definition API."""

from arango.api import GraphSpecificAPI
from arango.constants import HTTP_OK
from arango.exceptions import (
    EdgeDefinitionListError,
    EdgeDefinitionAddError,
    EdgeDefinitionReplaceError,
    EdgeDefinitionDeleteError,
)
from arango.utils import uncamelify


class EdgeDefinitionAPI(GraphSpecificAPI):
    """A wrapper around ArangoDB's edge definition API."""

    def __init__(self, connection, graph_name):
        super(EdgeDefinitionAPI, self).__init__(connection, graph_name)

    def list_edge_definitions(self):
        """Return the edge definitions of this graph.

        :returns: the edge definitions of this graph
        :rtype: list
        :raises: GraphPropertyError
        """
        res = self.conn.api_get(
            "/gharial/{}/".format(self.graph_name)
        )
        if res.status_code not in HTTP_OK:
            raise EdgeDefinitionListError(res)
        return uncamelify(res.obj["graph"])

    def create_edge_definition(self, edge_collection,
                               from_vertex_collections,
                               to_vertex_collections):
        """Add a edge definition to this graph.

        :param edge_collection: the name of the edge collection
        :type edge_collection: str
        :param from_vertex_collections: names of ``from`` vertex collections
        :type from_vertex_collections: list
        :param to_vertex_collections: the names of ``to`` vertex collections
        :type to_vertex_collections: list
        :returns: the updated list of edge definitions
        :rtype: list
        :raises: EdgeDefinitionAddError
        """
        res = self.conn.api_post(
            "/gharial/{}/edge".format(self.graph_name),
            data={
                "collection": edge_collection,
                "from": from_vertex_collections,
                "to": to_vertex_collections
            }
        )
        if res.status_code not in HTTP_OK:
            raise EdgeDefinitionAddError(res)
        return res.obj["graph"]["edgeDefinitions"]

    def replace_edge_definition(self, edge_collection,
                                from_vertex_collections,
                                to_vertex_collections):
        """Replace an edge definition in this graph.

        :param edge_collection: the name of the edge collection
        :type edge_collection: str
        :param from_vertex_collections: names of ``from`` vertex collections
        :type from_vertex_collections: list
        :param to_vertex_collections: the names of ``to`` vertex collections
        :type to_vertex_collections: list
        :returns: the updated list of edge definitions
        :rtype: list
        :raises: EdgeDefinitionReplaceError
        """
        res = self.conn.api_put(
            "/gharial/{}/edge/{}".format(
                self.graph_name, edge_collection
            ),
            data={
                "collection": edge_collection,
                "from": from_vertex_collections,
                "to": to_vertex_collections
            }
        )
        if res.status_code not in HTTP_OK:
            raise EdgeDefinitionReplaceError(res)
        return res.obj["graph"]["edgeDefinitions"]

    def delete_edge_definition(self, collection,
                               drop_collection=False):
        """Remove the specified edge definition from this graph.

        :param collection: the name of the edge collection to remove
        :type collection: str
        :param drop_collection: whether or not to drop the collection also
        :type drop_collection: bool
        :returns: the updated list of edge definitions
        :rtype: list
        :raises: EdgeDefinitionDeleteError
        """
        res = self.conn.api_delete(
            "/gharial/{}/edge/{}".format(self.graph_name, collection),
            params={"dropCollection": drop_collection}
        )
        if res.status_code != 200:
            raise EdgeDefinitionDeleteError(res)
        return res.obj["graph"]["edgeDefinitions"]
