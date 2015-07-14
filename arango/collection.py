"""Wrapper for ArangoDB's collection-specific APIs"""

from arango.utils import camelify, uncamelify
from arango.exceptions import *
from arango.constants import COLLECTION_STATUSES, HTTP_OK

from arango.api.bulk_import import BulkImportAPI
from arango.api.index import IndexAPI
from arango.api.simple_query import  SimpleQueryAPI


class Collection(BulkImportAPI, IndexAPI, SimpleQueryAPI):
    """A wrapper around ArangoDB collection specific API.

    :param connection: the ArangoDB connection object
    :type connection: arango.arangoapiconnection.ArangoConnection
    :param collection_name: the name of this collection
    :type collection_name: str
    """

    def __init__(self, connection, collection_name):
        super(Collection, self).__init__(connection, collection_name)

    def __setattr__(self, attr, value):
        """Modify the properties of this collection.

        Only ``wait_for_sync`` and ``journal_size`` are mutable.
        """
        if attr in {"wait_for_sync", "journal_size"}:
            res = self.conn.api_put(
                "/collection/{}/properties".format(self.col_name),
                data={camelify(attr): value}
            )
            if res.status_code not in HTTP_OK:
                raise CollectionUpdateError(res)
        else:
            super(Collection, self).__setattr__(attr, value)

    def __len__(self):
        """Return the number of documents in this collection.

        :returns: the number of documents
        :rtype: int
        :raises: CollectionPropertyError
        """
        res = self.conn.api_get(
            "/collection/{}/count".format(self.col_name)
        )
        if res.status_code not in HTTP_OK:
            raise CollectionPropertyError(res)
        return res.obj["count"]

    def __getitem__(self, key):
        """Return the document from this collection.

        :param key: the document key
        :type key: str
        :returns: the requested document
        :rtype: dict
        :raises: TypeError
        """
        if not isinstance(key, str):
            raise TypeError("Expecting a str.")
        return self.document(key)

    def __contains__(self, key):
        """Return True if the document exists in this collection.

        :param key: the document key
        :type key: str
        :returns: True if the document exists, else False
        :rtype: bool
        :raises: DocumentGetError
        """
        res = self.conn.api_head(
            "/{}/{}/{}".format(self._type, self.col_name, key)
        )
        if res.status_code == 200:
            return True
        elif res.status_code == 404:
            return False
        else:
            raise DocumentGetError(res)

    @property
    def id(self):
        """Return the ID of this collection.

        :returns: the ID of this collection
        :rtype: str
        :raises: CollectionPropertyError
        """
        return self.properties["id"]

    @property
    def status(self):
        """Return the status of this collection.

        :returns: the collection status
        :rtype: str
        :raises: CollectionPropertyError
        """
        return self.properties["status"]

    @property
    def key_options(self):
        """Return this collection's key options.

        :returns: the key options of this collection
        :rtype: dict
        :raises: CollectionPropertyError
        """
        return self.properties["key_options"]

    @property
    def wait_for_sync(self):
        """Return True if this collection waits for changes to sync to disk.

        :returns: True if collection waits for sync, False otherwise
        :rtype: bool
        :raises: CollectionPropertyError
        """
        return self.properties["wait_for_sync"]

    @property
    def journal_size(self):
        """Return the journal size of this collection.

        :returns: the journal size of this collection
        :rtype: str
        :raises: CollectionPropertyError
        """
        return self.properties["journal_size"]

    @property
    def is_volatile(self):
        """Return True if this collection is kept in memory and not persistent.

        :returns: True if the collection is volatile, False otherwise
        :rtype: bool
        :raises: CollectionPropertyError
        """
        return self.properties["is_volatile"]

    @property
    def is_system(self):
        """Return True if this collection is a system Collection.

        :returns: True if system collection, False otherwise
        :rtype: bool
        :raises: CollectionPropertyError
        """
        return self.properties["is_system"]

    @property
    def is_edge(self):
        """Return True if this collection is a system Collection.

        :returns: True if edge collection, False otherwise
        :rtype: bool
        :raises: CollectionPropertyError
        """
        return self.properties["is_edge"]

    @property
    def do_compact(self):
        """Return True if this collection is compacted.

        :returns: True if collection is compacted, False otherwise
        :rtype: bool
        :raises: CollectionPropertyError
        """
        return self.properties["do_compact"]

    @property
    def figures(self):
        """Return the statistics of this collection.

        :returns: the statistics of this collection
        :rtype: dict
        :raises: CollectionPropertyError
        """
        res = self.conn.api_get(
            "/collection/{}/figures".format(self.col_name)
        )
        if res.status_code not in HTTP_OK:
            raise CollectionPropertyError(res)
        return uncamelify(res.obj["figures"])

    @property
    def revision(self):
        """Return the revision of this collection.

        :returns: the collection revision (etag)
        :rtype: str
        :raises: CollectionPropertyError
        """
        res = self.conn.api_get(
            "/collection/{}/revision".format(self.col_name)
        )
        if res.status_code not in HTTP_OK:
            raise CollectionPropertyError(res)
        return res.obj["revision"]

    def load(self):
        """Load this collection into memory.

        :returns: the status of the collection
        :rtype: str
        :raises: CollectionLoadError
        """
        res = self.conn.api_put(
            "/collection/{}/load".format(self.col_name)
        )
        if res.status_code not in HTTP_OK:
            raise CollectionLoadError(res)
        return COLLECTION_STATUSES.get(
            res.obj["status"],
            "corrupted ({})".format(res.obj["status"])
        )

    def unload(self):
        """Unload this collection from memory.

        :returns: the status of the collection
        :rtype: str
        :raises: CollectionUnloadError
        """
        res = self.conn.api_put(
            "/collection/{}/unload".format(self.col_name)
        )
        if res.status_code not in HTTP_OK:
            raise CollectionUnloadError(res)
        return COLLECTION_STATUSES.get(
            res.obj["status"],
            "corrupted ({})".format(res.obj["status"])
        )

    def rotate_journal(self):
        """Rotate the journal of this collection.

        :raises: CollectionRotateJournalError
        """
        res = self.conn.api_put(
            "/collection/{}/rotate".format(self.col_name)
        )
        if res.status_code not in HTTP_OK:
            raise CollectionRotateJournalError(res)
        return res.obj["result"]

    def checksum(self, with_rev=False, with_data=False):
        """Return the checksum of this collection.

        :param with_rev: include the revision in the checksum calculation
        :type with_rev: bool
        :param with_data: include the data in the checksum calculation
        :type with_data: bool
        :returns: the checksum
        :rtype: int
        :raises: CollectionChecksumError
        """
        res = self.conn.api_get(
            "/collection/{}/checksum".format(self.col_name),
            params={"withRevision": with_rev, "withData": with_data}
        )
        if res.status_code not in HTTP_OK:
            raise CollectionChecksumError(res)
        return res.obj["checksum"]

    def truncate(self):
        """Remove all documents from this collection.

        :raises: CollectionTruncateError
        """
        res = self.conn.api_put(
            "/collection/{}/truncate".format(self.col_name)
        )
        if res.status_code not in HTTP_OK:
            raise CollectionTruncateError(res)
