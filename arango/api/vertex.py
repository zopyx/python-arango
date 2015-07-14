"""ArangoDB's vertex API."""

from arango.api import GraphSpecificAPI
from arango.constants import HTTP_OK
from arango.exceptions import (
    VertexRevisionError,
    VertexGetError,
    VertexCreateError,
    VertexDeleteError,
    VertexUpdateError,
    VertexReplaceError,
)


class VertexAPI(GraphSpecificAPI):
    """A wrapper around ArangoDB's vertex API."""

    def __init__(self, connection, graph_name):
        super(VertexAPI, self).__init__(connection, graph_name)

    def get_vertex(self, vertex_id, rev=None):
        """Return the vertex of the specified ID in this graph.

        If the vertex revision ``rev`` is specified, it must match against
        the revision of the retrieved vertex.

        :param vertex_id: the ID of the vertex to retrieve
        :type vertex_id: str
        :param rev: the vertex revision must match this value
        :type rev: str or None
        :returns: the requested vertex or None if not found
        :rtype: dict or None
        :raises: DocumentRevisionError, VertexGetError
        """
        res = self.conn.api_get(
            "/gharial/{}/vertex/{}".format(self.graph_name, vertex_id),
            params={"rev": rev} if rev is not None else {}
        )
        if res.status_code == 412:
            raise VertexRevisionError(res)
        elif res.status_code == 404:
            return None
        elif res.status_code not in HTTP_OK:
            raise VertexGetError(res)
        return res.obj["vertex"]

    def create_vertex(self, collection, data, wait_for_sync=False,
                      _batch=False):
        """Add a vertex to the specified vertex collection if this graph.

        If ``data`` contains the ``_key`` key, its value must be unused
        in the collection.

        :param collection: the name of the vertex collection
        :type collection: str
        :param data: the body of the new vertex
        :type data: dict
        :param wait_for_sync: wait for the add to sync to disk
        :type wait_for_sync: bool
        :return: the id, rev and key of the new vertex
        :rtype: dict
        :raises: VertexCreateError
        """
        api_path = "/gharial/{}/vertex/{}".format(self.graph_name, collection)
        params = {"waitForSync": wait_for_sync}
        if _batch:
            return {
                "method": "post",
                "path": api_path,
                "data": data,
                "params": params,
            }
        res = self.conn.api_post(api_path=api_path, data=data, params=params)
        if res.status_code not in HTTP_OK:
            raise VertexCreateError(res)
        return res.obj["vertex"]

    def update_vertex(self, vertex_id, data, rev=None, keep_none=True,
                      wait_for_sync=False, _batch=False):
        """Update a vertex of the specified ID in this graph.

        If ``keep_none`` is set to True, then attributes with values None
        are retained. Otherwise, they are removed from the vertex.

        If ``data`` contains the ``_key`` key, it is ignored.

        If the ``_rev`` key is in ``data``, the revision of the target
        vertex must match against its value. Otherwise a RevisionMismatch
        error is thrown. If ``rev`` is also provided, its value is preferred.

        :param vertex_id: the ID of the vertex to be updated
        :type vertex_id: str
        :param data: the body to update the vertex with
        :type data: dict
        :param rev: the vertex revision must match this value
        :type rev: str or None
        :param keep_none: whether or not to keep the keys with value None
        :type keep_none: bool
        :param wait_for_sync: wait for the update to sync to disk
        :type wait_for_sync: bool
        :return: the id, rev and key of the updated vertex
        :rtype: dict
        :raises: DocumentRevisionError, VertexUpdateError
        """
        api_path = "/gharial/{}/vertex/{}".format(self.graph_name, vertex_id)
        params = {
            "waitForSync": wait_for_sync,
            "keepNull": keep_none
        }
        if rev is not None:
            params["rev"] = rev
        elif "_rev" in data:
            params["rev"] = data["_rev"]
        if _batch:
            return {
                "method": "patch",
                "path": api_path,
                "data": data,
                "params": params,
            }
        res = self.conn.api_patch(api_path=api_path, data=data, params=params)
        if res.status_code == 412:
            raise VertexRevisionError(res)
        elif res.status_code not in HTTP_OK:
            raise VertexUpdateError(res)
        return res.obj["vertex"]

    def replace_vertex(self, vertex_id, data, rev=None, wait_for_sync=False,
                       _batch=False):
        """Replace a vertex of the specified ID in this graph.

        If ``data`` contains the ``_key`` key, it is ignored.

        If the ``_rev`` key is in ``data``, the revision of the target
        vertex must match against its value. Otherwise a RevisionMismatch
        error is thrown. If ``rev`` is also provided, its value is preferred.

        :param vertex_id: the ID of the vertex to be replaced
        :type vertex_id: str
        :param data: the body to replace the vertex with
        :type data: dict
        :param rev: the vertex revision must match this value
        :type rev: str or None
        :param wait_for_sync: wait for replace to sync to disk
        :type wait_for_sync: bool
        :return: the id, rev and key of the replaced vertex
        :rtype: dict
        :raises: DocumentRevisionError, VertexReplaceError
        """
        api_path = "/gharial/{}/vertex/{}".format(self.graph_name, vertex_id)
        params = {"waitForSync": wait_for_sync}
        if rev is not None:
            params["rev"] = rev
        if "_rev" in data:
            params["rev"] = data["_rev"]
        if _batch:
            return {
                "method": "put",
                "path": api_path,
                "data": data,
                "params": params,
            }
        res = self.conn.api_put(api_path=api_path, params=params, data=data)
        if res.status_code == 412:
            raise VertexRevisionError(res)
        elif res.status_code not in HTTP_OK:
            raise VertexReplaceError(res)
        return res.obj["vertex"]

    def delete_vertex(self, vertex_id, rev=None, wait_for_sync=False,
                      _batch=False):
        """Remove the vertex of the specified ID from this graph.

        :param vertex_id: the ID of the vertex to be removed
        :type vertex_id: str
        :param rev: the vertex revision must match this value
        :type rev: str or None
        :raises: DocumentRevisionError, VertexDeleteError
        """
        api_path = "/gharial/{}/vertex/{}".format(self.graph_name, vertex_id)
        params = {"waitForSync": wait_for_sync}
        if rev is not None:
            params["rev"] = rev
        if _batch:
            return {
                "method": "delete",
                "path": api_path,
                "params": params,
            }
        res = self.conn.api_delete(api_path=api_path, params=params)
        if res.status_code == 412:
            raise VertexRevisionError(res)
        if res.status_code not in HTTP_OK:
            raise VertexDeleteError(res)
        return res.obj
