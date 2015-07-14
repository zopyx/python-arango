"""ArangoDB's bulk import API."""

import json
from arango.api import CollectionSpecificAPI
from arango.constants import HTTP_OK
from arango.exceptions import BulkImportError


class BulkImportAPI(CollectionSpecificAPI):
    """A wrapper around ArangoDB's bulk import API."""

    def __init__(self, connection, collection_name):
        super(BulkImportAPI, self).__init__(connection, collection_name)

    def bulk_import(self, documents, complete=True, details=True):
        """Import documents into this collection in bulk.

        If ``complete`` is set to a value other than True, valid documents
        will be imported while invalid ones are rejected, meaning only some of
        the uploaded documents might have been imported.

        If ``details`` parameter is set to True, the response will also contain
        ``details`` attribute which is a list of detailed error messages.

        :param documents: list of documents to import
        :type documents: list
        :param complete: entire import fails if any document is invalid
        :type complete: bool
        :param details: return details about invalid documents
        :type details: bool
        :returns: the import results
        :rtype: dict
        :raises: BulkImportError
        """
        res = self.conn.api_post(
            "/import",
            data="\r\n".join([json.dumps(d) for d in documents]),
            params={
                "type": "documents",
                "collection": self.col_name,
                "complete": complete,
                "details": details
            }
        )
        if res.status_code not in HTTP_OK:
            raise BulkImportError(res)
        del res.obj["error"]
        return res.obj
