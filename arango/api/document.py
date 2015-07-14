"""ArangoDB's document API."""

from arango.api import CollectionSpecificAPI
from arango.constants import HTTP_OK
from arango.exceptions import (
    DocumentGetError,
    DocumentRevisionError,
    DocumentAddError,
    DocumentUpdateError,
    DocumentReplaceError,
    DocumentDeleteError,
)


class DocumentAPI(CollectionSpecificAPI):
    """A wrapper around ArangoDB's document API."""

    def __init__(self, connection, collection_name, collection_type="document"):
        super(DocumentAPI, self).__init__(connection, collection_name)
        self.col_type = collection_type

    def get_document(self, key, rev=None, match=True):
        """Return the document of the given key.

        If the document revision ``rev`` is specified, it is compared
        against the revision of the retrieved document. If ``match`` is set
        to True and the revisions do NOT match, or if ``match`` is set to
        False and the revisions DO match, ``DocumentRevisionError`` is thrown.

        :param key: the key of the document to retrieve
        :type key: str
        :param rev: the document revision is compared against this value
        :type rev: str or None
        :param match: whether or not the revision should match
        :type match: bool
        :returns: the requested document or None if not found
        :rtype: dict or None
        :raises: DocumentRevisionError, DocumentGetError
        """
        res = self.conn.api_get(
            "/{}/{}/{}".format(self.col_type, self.col_name, key),
            headers={
                "If-Match" if match else "If-None-Match": rev
            } if rev else {}
        )
        if res.status_code in {412, 304}:
            raise DocumentRevisionError(res)
        elif res.status_code == 404:
            return None
        elif res.status_code not in HTTP_OK:
            raise DocumentGetError(res)
        return res.obj

    def create_document(self, data, wait_for_sync=False, _batch=False):
        """Add the new document to the collection.

        If ``data`` contains the ``_key`` key, its value must be free.
        If this collection is an edge collection, ``data`` must contain the
        ``_from`` and ``_to`` keys with valid vertex IDs as their values.

        :param data: the body of the new document
        :type data: dict
        :param wait_for_sync: wait for add to sync to disk
        :type wait_for_sync: bool
        :returns: the id, rev and key of the new document
        :rtype: dict
        :raises: DocumentInvalidError, DocumentAddError
        """
        path = "/{}".format(self.col_type)
        params = {
            "collection": self.col_name,
            "waitForSync": wait_for_sync,
        }
        if "_from" in data:
            params["from"] = data["_from"]
        if "_to" in data:
            params["to"] = data["_to"]
        if _batch:
            return {
                "method": "post",
                "path": path,
                "data": data,
                "params": params,
            }
        res = self.conn.api_post(api_path=path, data=data, params=params)
        if res.status_code not in HTTP_OK:
            raise DocumentAddError(res)
        return res.obj

    def update_document(self, key, data, rev=None, keep_none=True,
                        wait_for_sync=False, _batch=False):
        """Update the specified document in this collection.

        If ``keep_none`` is set to True, then attributes with values None
        are retained. Otherwise, they are removed from the document.

        If ``data`` contains the ``_key`` key, it is ignored.

        If the ``_rev`` key is in ``data``, the revision of the target
        document must match against its value. Otherwise a RevisionMismatch
        error is thrown. If ``rev`` is also provided, its value is preferred.

        The ``_from`` and ``_to`` attributes are immutable, and they are
        ignored if present in ``data``

        :param key: the key of the document to be updated
        :type key: str
        :param data: the body to update the document with
        :type data: dict
        :param rev: the document revision must match this value
        :type rev: str or None
        :param keep_none: whether or not to keep the items with value None
        :type keep_none: bool
        :param wait_for_sync: wait for the update to sync to disk
        :type wait_for_sync: bool
        :returns: the id, rev and key of the updated document
        :rtype: dict
        :raises: DocumentUpdateError
        """
        path = "/{}/{}/{}".format(self.col_type, self.col_name, key)
        params = {
            "waitForSync": wait_for_sync,
            "keepNull": keep_none
        }
        if rev is not None:
            params["rev"] = rev
            params["policy"] = "error"
        elif "_rev" in data:
            params["rev"] = data["_rev"]
            params["policy"] = "error"
        if _batch:
            return {
                "method": "patch",
                "path": path,
                "data": data,
                "params": params,
            }
        res = self.conn.api_patch(api_path=path, data=data, params=params)
        if res.status_code == 412:
            raise DocumentRevisionError(res)
        if res.status_code not in HTTP_OK:
            raise DocumentUpdateError(res)
        del res.obj["error"]
        return res.obj

    def replace_document(self, key, data, rev=None, wait_for_sync=False,
                         _batch=False):
        """Replace the specified document in this collection.

        If ``data`` contains the ``_key`` key, it is ignored.

        If the ``_rev`` key is in ``data``, the revision of the target
        document must match against its value. Otherwise a RevisionMismatch
        error is thrown. If ``rev`` is also provided, its value is preferred.

        The ``_from`` and ``_to`` attributes are immutable, and they are
        ignored if present in ``data``.

        :param key: the key of the document to be replaced
        :type key: str
        :param data: the body to replace the document with
        :type data: dict
        :param rev: the document revision must match this value
        :type rev: str or None
        :param wait_for_sync: wait for the replace to sync to disk
        :type wait_for_sync: bool
        :returns: the id, rev and key of the replaced document
        :rtype: dict
        :raises: DocumentReplaceError
        """
        params = {"waitForSync": wait_for_sync}
        if rev is not None:
            params["rev"] = rev
            params["policy"] = "error"
        elif "_rev" in data:
            params["rev"] = data["_rev"]
            params["policy"] = "error"
        path = "/{}/{}/{}".format(self.col_type, self.col_name, key)
        if _batch:
            return {
                "method": "put",
                "path": path,
                "data": data,
                "params": params,
            }
        res = self.conn.api_put(api_path=path, params=params, data=data)
        if res.status_code == 412:
            raise DocumentRevisionError(res)
        elif res.status_code not in HTTP_OK:
            raise DocumentReplaceError(res)
        del res.obj["error"]
        return res.obj

    def delete_document(self, key, rev=None, wait_for_sync=False, _batch=False):
        """Remove the specified document from this collection.

        :param key: the key of the document to be removed
        :type key: str
        :param rev: the document revision must match this value
        :type rev: str or None
        :param wait_for_sync: wait for the remove to sync to disk
        :type wait_for_sync: bool
        :returns: the id, rev and key of the deleted document
        :rtype: dict
        :raises: DocumentRevisionError, DocumentDeleteError
        """
        params = {"waitForSync": wait_for_sync}
        if rev is not None:
            params["rev"] = rev
            params["policy"] = "error"
        path = "/{}/{}/{}".format(self.col_type, self.col_name, key)
        if _batch:
            return {
                "method": "delete",
                "path": path,
                "params": params
            }
        res = self.conn.api_delete(api_path=path, params=params)
        if res.status_code == 412:
            raise DocumentRevisionError(res)
        elif res.status_code not in HTTP_OK:
            raise DocumentDeleteError(res)
        del res.obj["error"]
        return res.obj
