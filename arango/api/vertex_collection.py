"""ArangoDB's vertex collection API."""

from arango.api import GraphSpecificAPI
from arango.constants import HTTP_OK
from arango.exceptions import (
    VertexCollectionAllError,
    VertexCollectionCreateError,
    VertexCollectionDeleteError,
)


class VertexCollectionAPI(GraphSpecificAPI):
    """A wrapper around ArangoDB's vertex collection API.

    :param connection: ArangoDB connection object
    :type connection: arango.connection.ArangoConnection
    :param database_name: name of the database where the graph is in
    :type database_name: str
    :param graph_name: name of the graph to direct the API calls to
    :type graph_name: str
    """

    def __init__(self, connection, database_name, graph_name):
        super(VertexCollectionAPI, self).__init__(
            connection,
            database_name,
            graph_name
        )

    def all_vertex_collections(self):
        """Return the details on all vertex collections.

        :returns: details on all vertex collections
        :rtype: list
        :raises: VertexCollectionAllError
        """
        api_path = "/_api/gharial/{}/vertex".format(self.graph_name)
        res = self.conn.api_get(self.db_name, api_path)
        if res.status_code not in HTTP_OK:
            raise VertexCollectionAllError(res)
        return res.obj["collections"]

    def create_vertex_collection(self, collection_name):
        """Create a vertex collection.

        :param collection_name: the name of the vertex collection to create
        :type collection_name: str
        :returns: the updated list of the vertex collection names
        :rtype: list
        :raises: VertexCollectionCreateError
        """
        api_path = "/_api/gharial/{}/vertex".format(self.graph_name)
        res = self.conn.api_post(
            self.db_name,
            api_path,
            data={"collection": collection_name}
        )
        if res.status_code not in HTTP_OK:
            raise VertexCollectionCreateError(res)
        return res.obj

    def delete_vertex_collection(self, collection_name, drop_collection=False):
        """Delete a vertex collection.

        This simply detaches the collection from the graph. In order to delete
        the collection as well ``drop_collection`` must be set to True.

        :param collection_name: the name of the vertex collection to remove
        :type collection_name: str
        :param drop_collection: whether or not to drop the collection also
        :type drop_collection: bool
        :returns: the updated list of the vertex collection names
        :rtype: list
        :raises: VertexCollectionDeleteError
        """
        res = self.conn.api_delete(
            "/gharial/{}/vertex/{}".format(self.graph_name, collection_name),
            params={"dropCollection": drop_collection}
        )
        if res.status_code not in HTTP_OK:
            raise VertexCollectionDeleteError(res)
        return res.obj
