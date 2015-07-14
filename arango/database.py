"""ArangoDB's database-specific API."""

from arango.constants import HTTP_OK
from arango.utils import uncamelify
from arango.api.database import (
    AQLFunctionAPI,
    AQLQueryAPI,
    BatchAPI,
    CollectionManagementAPI,
    GraphManagementAPI,
    TransactionAPI
)
from arango.exceptions import DatabasePropertyError


class Database(AQLFunctionAPI, AQLQueryAPI, BatchAPI, CollectionManagementAPI,
               GraphManagementAPI, TransactionAPI):
    """A wrapper around database specific API.

    :param connection: the ArangoDB connection object
    :type connection: arango.connection.ArangoConnection
    :param database_name: the name of this database
    :type database_name: str
    """

    def __init__(self, connection, database_name):
        super(Database, self).__init__(connection, database_name)
        self._collection_cache = {}
        self._graph_cache = {}


    def _update_collection_cache(self):
        """Invalidate the collection cache."""
        real_cols = set(self.collections["all"])
        cached_cols = set(self._collection_cache)
        for col_name in cached_cols - real_cols:
            del self._collection_cache[col_name]
        for col_name in real_cols - cached_cols:
            self._collection_cache[col_name] = Collection(
                connection=self.conn, collection_name=col_name
            )

    def __getattr__(self, attr):
        """Call __getattr__ of the default database."""
        return getattr(self._default_database, attr)

    def collection(self, name):
        """Return the Collection object of the specified name.

        :param name: the name of the collection
        :type name: str
        :returns: the requested collection object
        :rtype: arango.collection.Collection
        :raises: TypeError, CollectionNotFound
        """
        if not isinstance(name, str):
            raise TypeError("Expecting a str.")
        if name in self._collection_cache:
            return self._collection_cache[name]
        else:
            self._update_collection_cache()
            if name not in self._collection_cache:
                raise CollectionNotFoundError(name)
            return self._collection_cache[name]



    @property
    def properties(self):
        """Return all properties of this database.

        :returns: the database properties
        :rtype: dict
        :raises: DatabasePropertyError
        """
        res = self.conn.api_get("/database/current")
        if res.status_code not in HTTP_OK:
            raise DatabasePropertyError(res)
        return uncamelify(res.obj["result"])

    @property
    def id(self):
        """Return the ID of this database.

        :returns: the database ID
        :rtype: str
        :raises: DatabasePropertyError
        """
        return self.properties["id"]

    @property
    def path(self):
        """Return the file path of this database.

        :returns: the file path of this database
        :rtype: str
        :raises: DatabasePropertyError
        """
        return self.properties["path"]

    @property
    def is_system(self):
        """Return True if this is a system database, False otherwise.

        :returns: True if this is a system database, False otherwise
        :rtype: bool
        :raises: DatabasePropertyError
        """
        return self.properties["is_system"]




    def _update_graph_cache(self):
        """Invalidate the graph cache."""
        real_graphs = set(self.graphs)
        cached_graphs = set(self._graph_cache)
        for graph_name in cached_graphs - real_graphs:
            del self._graph_cache[graph_name]
        for graph_name in real_graphs - cached_graphs:
            self._graph_cache[graph_name] = Graph(
                connection=self.conn, name=graph_name
            )

    @property
    def graphs(self):
        """List all graphs in this database.

        :returns: the graphs in this database
        :rtype: dict
        :raises: GraphGetError
        """
        res = self.conn.api_get("/gharial")
        if res.status_code not in (200, 202):
            raise GraphListError(res)
        return [graph["_key"] for graph in res.obj["graphs"]]

    def graph(self, name):
        """Return the Graph object of the specified name.

        :param name: the name of the graph
        :type name: str
        :returns: the requested graph object
        :rtype: arango.graph.Graph
        :raises: TypeError, GraphNotFound
        """
        if not isinstance(name, str):
            raise TypeError("Expecting a str.")
        if name in self._graph_cache:
            return self._graph_cache[name]
        else:
            self._update_graph_cache()
            if name not in self._graph_cache:
                raise GraphNotFoundError(name)
            return self._graph_cache[name]

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
        if res.status_code != 201:
            raise GraphCreateError(res)
        self._update_graph_cache()
        return self.graph(name)

    def delete_graph(self, name):
        """Delete the graph of the given name from this database.

        :param name: the name of the graph to delete
        :type name: str
        :raises: GraphDeleteError
        """
        res = self.conn.api_delete("/gharial/{}".format(name))
        if res.status_code != 200:
            raise GraphDeleteError(res)
        self._update_graph_cache()