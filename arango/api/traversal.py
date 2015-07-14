"""ArangoDB's graph traversal API."""

from arango.api import GraphSpecificAPI
from arango.constants import HTTP_OK
from arango.exceptions import GraphTraversalError


class TraversalAPI(GraphSpecificAPI):
    """A wrapper around ArangoDB's traversal API."""

    def __init__(self, connection, graph_name):
        super(TraversalAPI, self).__init__(connection, graph_name)

    def execute_traversal(self, start_vertex, direction=None, strategy=None,
                          order=None, item_order=None, uniqueness=None,
                          max_iterations=None, min_depth=None, max_depth=None,
                          init=None, filters=None, visitor=None, expander=None,
                          sort=None):
        """Execute a graph traversal and return the visited vertexes.

        For more details on ``init``, ``filter``, ``visitor``, ``expander``
        and ``sort`` please refer to the ArangoDB HTTP API documentation:
        https://docs.arangodb.com/HttpTraversal/README.html

        :param start_vertex: the ID of the start vertex
        :type start_vertex: str
        :param direction: "outbound" or "inbound" or "any"
        :type direction: str or None
        :param strategy: "depthfirst" or "breadthfirst"
        :type strategy: str or None
        :param order: "preorder" or "postorder"
        :type order: str or None
        :param item_order: "forward" or "backward"
        :type item_order: str or None
        :param uniqueness: uniqueness of vertexes and edges visited
        :type uniqueness: dict or None
        :param max_iterations: max number of iterations in each traversal
        :type max_iterations: int or None
        :param min_depth: minimum traversal depth
        :type min_depth: int or None
        :param max_depth: maximum traversal depth
        :type max_depth: int or None
        :param init: custom init function in Javascript
        :type init: str or None
        :param filters: custom filter function in Javascript
        :type filters: str or None
        :param visitor: custom visitor function in Javascript
        :type visitor: str or None
        :param expander: custom expander function in Javascript
        :type expander: str or None
        :param sort: custom sorting function in Javascript
        :type sort: str or None
        :returns: the traversal results
        :rtype: dict or None
        :raises: GraphTraversalError
        """
        data = {
            "startVertex": start_vertex,
            "graphName": self.graph_name,
            "direction": direction,
            "strategy": strategy,
            "order": order,
            "itemOrder": item_order,
            "uniqueness": uniqueness,
            "maxIterations": max_iterations,
            "minDepth": min_depth,
            "maxDepth": max_depth,
            "init": init,
            "filter": filters,
            "visitor": visitor,
            "expander": expander,
            "sort": sort
        }
        data = {k: v for k, v in data.items() if v is not None}
        res = self.conn.api_post("/traversal", data=data)
        if res.status_code not in HTTP_OK:
            raise GraphTraversalError(res)
        return res.obj["result"]
