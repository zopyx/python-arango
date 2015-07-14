"""ArangoDB's index API."""

from arango.utils import uncamelify
from arango.api import CollectionSpecificAPI
from arango.constants import HTTP_OK
from arango.exceptions import (
    IndexCreateError,
    IndexListError,
    IndexDeleteError,
)


class IndexAPI(CollectionSpecificAPI):
    """A wrapper around ArangoDB's index API."""

    def __init__(self, connection, collection_name):
        super(IndexAPI, self).__init__(connection, collection_name)

    def list_indexes(self):
        """Return the details on the indexes of this collection.

        :returns: the index details
        :rtype: dict
        :raises: IndexListError
        """
        res = self.conn.api_get(
            "/index?collection={}".format(self.col_name)
        )
        if res.status_code not in HTTP_OK:
            raise IndexListError(res)

        indexes = {}
        for index_id, details in res.obj["identifiers"].items():
            del details["id"]
            indexes[index_id.split("/", 1)[1]] = uncamelify(details)
        return indexes

    def _create_index(self, data):
        """Helper method for adding new indexes."""
        res = self.conn.api_post(
            "/index?collection={}".format(self.col_name),
            data=data
        )
        if res.status_code not in HTTP_OK:
            raise IndexCreateError(res)
        return res.obj

    def create_hash_index(self, fields, unique=None, sparse=None):
        """Add a new hash index to this collection.

        :param fields: the attribute paths to index
        :type fields: list
        :param unique: whether or not the index is unique
        :type unique: bool or None
        :param sparse: whether to index attr values of null
        :type sparse: bool or None
        :raises: IndexCreateError
        """
        data = {"type": "hash", "fields": fields}
        if unique is not None:
            data["unique"] = unique
        if sparse is not None:
            data["sparse"] = sparse
        return self._create_index(data)

    def create_cap_constraint(self, size=None, byte_size=None):
        """Add a cap constraint to this collection.

        :param size: the number for documents allowed in this collection
        :type size: int or None
        :param byte_size: the max size of the active document data (> 16384)
        :type byte_size: int or None
        :raises: IndexCreateError
        """
        data = {"type": "cap"}
        if size is not None:
            data["size"] = size
        if byte_size is not None:
            data["byteSize"] = byte_size
        return self._create_index(data)

    def create_skiplist_index(self, fields, unique=None, sparse=None):
        """Add a new skiplist index to this collection.

        A skiplist index is used to find ranges of documents (e.g. time).

        :param fields: the attribute paths to index
        :type fields: list
        :param unique: whether or not the index is unique
        :type unique: bool or None
        :param sparse: whether to index attr values of null
        :type sparse: bool or None
        :raises: IndexCreateError
        """
        data = {"type": "skiplist", "fields": fields}
        if unique is not None:
            data["unique"] = unique
        if sparse is not None:
            data["sparse"] = sparse
        return self._create_index(data)

    def create_geo_index(self, fields, geo_json=None, unique=None,
                         ignore_null=None):
        """Add a geo index to this collection

        If ``fields`` is a list with ONE attribute path, then a geo-spatial
        index on all documents is created using the value at the path as the
        coordinates. The value must be a list with at least two doubles. The
        list must contain the latitude (first value) and the longitude (second
        value). All documents without the attribute paths or with invalid values
        are ignored.

        If ``fields`` is a list with TWO attribute paths (i.e. latitude and
        longitude, in that order) then a geo-spatial index on all documents is
        created using the two attributes (again, their values must be doubles).
        All documents without the attribute paths or with invalid values are
        ignored.

        :param fields: the attribute paths to index (length must be <= 2)
        :type fields: list
        :param geo_json: whether or not the order is longitude -> latitude
        :type geo_json: bool or None
        :param unique: whether or not to create a geo-spatial constraint
        :type unique: bool or None
        :param ignore_null: ignore docs with None in latitude/longitude
        :type ignore_null: bool or None
        :raises: IndexCreateError
        """
        data = {"type": "geo", "fields": fields}
        if geo_json is not None:
            data["geoJson"] = geo_json
        if unique is not None:
            data["unique"] = unique
        if ignore_null is not None:
            data["ignore_null"] = ignore_null
        return self._create_index(data)

    def create_fulltext_index(self, fields, min_length=None):
        """Add a fulltext index to this collection.

        A fulltext index can be used to find words, or prefixes of words inside
        documents. A fulltext index can be set on one attribute only, and will
        index all words contained in documents that have a textual value in this
        attribute. Only words with a (specifiable) minimum length are indexed.
        Word tokenization is done using the word boundary analysis provided by
        libicu, which is taking into account the selected language provided at
        server start. Words are indexed in their lower-cased form. The index
        supports complete match queries (full words) and prefix queries.

        Fulltext index cannot be unique.

        :param fields: the attribute paths to index (length must be 1)
        :type fields: list
        :param min_length: minimum character length of words to index
        :type min_length: int
        :raises: IndexCreateError
        """
        data = {"type": "fulltext", "fields": fields}
        if min_length is not None:
            data["minLength"] = min_length
        return self._create_index(data)

    def delete_index(self, index_id):
        """Delete an index from this collection.

        :param index_id: the ID of the index to remove
        :type index_id: str
        :raises: IndexDeleteError
        """
        res = self.conn.api_delete(
            "/index/{}/{}".format(self.col_name, index_id)
        )
        if res.status_code not in HTTP_OK:
            raise IndexDeleteError(res)
        return res.obj
