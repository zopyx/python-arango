"""ArangoDB's edge API."""

from arango.api import GraphSpecificAPI
from arango.constants import HTTP_OK
from arango.exceptions import (
    EdgeRevisionError,
    EdgeGetError,
    EdgeInvalidError,
    EdgeCreateError,
    EdgeDeleteError,
    EdgeUpdateError,
    EdgeReplaceError,
)


class EdgeAPI(GraphSpecificAPI):
    """A wrapper around ArangoDB's edge API."""

    def __init__(self, connection, graph_name):
        super(EdgeAPI, self).__init__(connection, graph_name)

    def get_edge(self, edge_id, rev=None):
        """Return the edge of the specified ID in this graph.

        If the edge revision ``rev`` is specified, it must match against
        the revision of the retrieved edge.

        :param edge_id: the ID of the edge to retrieve
        :type edge_id: str
        :param rev: the edge revision must match this value
        :type rev: str or None
        :returns: the requested edge or None if not found
        :rtype: dict or None
        :raises: DocumentRevisionError, EdgeGetError
        """
        res = self.conn.api_get(
            "/gharial/{}/edge/{}".format(self.graph_name, edge_id),
            params={} if rev is None else {"rev": rev}
        )
        if res.status_code == 412:
            raise EdgeRevisionError(res)
        elif res.status_code == 404:
            return None
        elif res.status_code not in HTTP_OK:
            raise EdgeGetError(res)
        return res.obj["edge"]

    def create_edge(self, collection, data, wait_for_sync=False, _batch=False):
        """Add an edge to the specified edge collection of this graph.

        The ``data`` must contain ``_from`` and ``_to`` keys with valid
        vertex IDs as their values. If ``data`` contains the ``_key`` key,
        its value must be unused in the collection.

        :param collection: the name of the edge collection
        :type collection: str
        :param data: the body of the new edge
        :type data: dict
        :param wait_for_sync: wait for the add to sync to disk
        :type wait_for_sync: bool
        :return: the id, rev and key of the new edge
        :rtype: dict
        :raises: DocumentInvalidError, EdgeCreateError
        """
        if "_to" not in data:
            raise EdgeInvalidError(
                "the new edge data is missing the '_to' key")
        if "_from" not in data:
            raise EdgeInvalidError(
                "the new edge data is missing the '_from' key")
        api_path = "/gharial/{}/edge/{}".format(self.graph_name, collection)
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
            raise EdgeCreateError(res)
        return res.obj["edge"]

    def update_edge(self, edge_id, data, rev=None, keep_none=True,
                    wait_for_sync=False, _batch=False):
        """Update the edge of the specified ID in this graph.

        If ``keep_none`` is set to True, then attributes with values None
        are retained. Otherwise, they are removed from the edge.

        If ``data`` contains the ``_key`` key, it is ignored.

        If the ``_rev`` key is in ``data``, the revision of the target
        edge must match against its value. Otherwise a RevisionMismatch
        error is thrown. If ``rev`` is also provided, its value is preferred.

        The ``_from`` and ``_to`` attributes are immutable, and they are
        ignored if present in ``data``

        :param edge_id: the ID of the edge to be removed
        :type edge_id: str
        :param data: the body to update the edge with
        :type data: dict
        :param rev: the edge revision must match this value
        :type rev: str or None
        :param keep_none: whether or not to keep the keys with value None
        :type keep_none: bool
        :param wait_for_sync: wait for the update to sync to disk
        :type wait_for_sync: bool
        :return: the id, rev and key of the updated edge
        :rtype: dict
        :raises: DocumentRevisionError, EdgeUpdateError
        """
        api_path = "/gharial/{}/edge/{}".format(self.graph_name, edge_id)
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
            raise EdgeRevisionError(res)
        elif res.status_code not in HTTP_OK:
            raise EdgeUpdateError(res)
        return res.obj["edge"]

    def replace_edge(self, edge_id, data, rev=None, wait_for_sync=False,
                     _batch=False):
        """Replace the edge of the specified ID in this graph.

        If ``data`` contains the ``_key`` key, it is ignored.

        If the ``_rev`` key is in ``data``, the revision of the target
        edge must match against its value. Otherwise a RevisionMismatch
        error is thrown. If ``rev`` is also provided, its value is preferred.

        The ``_from`` and ``_to`` attributes are immutable, and they are
        ignored if present in ``data``

        :param edge_id: the ID of the edge to be removed
        :type edge_id: str
        :param data: the body to replace the edge with
        :type data: dict
        :param rev: the edge revision must match this value
        :type rev: str or None
        :param wait_for_sync: wait for the replace to sync to disk
        :type wait_for_sync: bool
        :returns: the id, rev and key of the replaced edge
        :rtype: dict
        :raises: DocumentRevisionError, EdgeReplaceError
        """
        api_path = "/gharial/{}/edge/{}".format(self.graph_name, edge_id)
        params = {"waitForSync": wait_for_sync}
        if rev is not None:
            params["rev"] = rev
        elif "_rev" in data:
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
            raise EdgeRevisionError(res)
        elif res.status_code not in HTTP_OK:
            raise EdgeReplaceError(res)
        return res.obj["edge"]

    def delete_edge(self, edge_id, rev=None, wait_for_sync=False,
                    _batch=False):
        """Remove the edge of the specified ID from this graph.

        :param edge_id: the ID of the edge to be removed
        :type edge_id: str
        :param rev: the edge revision must match this value
        :type rev: str or None
        :raises: DocumentRevisionError, EdgeDeleteError
        """
        api_path = "/gharial/{}/edge/{}".format(self.graph_name, edge_id)
        params = {"waitForSync": wait_for_sync}
        if _batch:
            return {
                "method": "delete",
                "path": api_path,
                "params": params,
            }
        if rev is not None:
            params["rev"] = rev
        res = self.conn.api_delete(api_path=api_path, params=params)
        if res.status_code == 412:
            raise EdgeRevisionError(res)
        elif res.status_code not in HTTP_OK:
            raise EdgeDeleteError(res)
