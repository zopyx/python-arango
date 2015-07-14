"""ArangoDB's simple query API."""

from arango.api.cursor import CursorAPI
from arango.api import CollectionSpecificAPI
from arango.constants import HTTP_OK
from arango.exceptions import (
    SimpleQueryRemoveByKeysError,
    SimpleQueryAllError,
    SimpleQueryAnyError,
    SimpleQueryFirstError,
    SimpleQueryFirstExampleError,
    SimpleQueryFullTextError,
    SimpleQueryGetByExampleError,
    SimpleQueryLastError,
    SimpleQueryLookupByKeysError,
    SimpleQueryNearError,
    SimpleQueryRangeError,
    SimpleQueryRemoveByExampleError,
    SimpleQueryReplaceByExampleError,
    SimpleQueryUpdateByExampleError,
    SimpleQueryWithinError,
)


class SimpleQueryAPI(CollectionSpecificAPI, CursorAPI):
    """A wrapper around ArangoDB's simple query API."""

    def __init__(self, connection, collection_name):
        super(SimpleQueryAPI, self).__init__(connection, collection_name)

    def first(self, count=1):
        """Return the first ``count`` number of documents in this collection.

        :param count: the number of documents to return
        :type count: int
        :returns: the list of documents
        :rtype: list
        :raises: SimpleQueryFirstError
        """
        res = self.conn.api_put(
            "/simple/first",
            data={"collection": self.col_name, "count": count}
        )
        if res.status_code not in HTTP_OK:
            raise SimpleQueryFirstError(res)
        return res.obj["result"]

    def last(self, count=1):
        """Return the last ``count`` number of documents in this collection.

        :param count: the number of documents to return
        :type count: int
        :returns: the list of documents
        :rtype: list
        :raises: SimpleQueryLastError
        """
        res = self.conn.api_put(
            "/simple/last",
            data={"collection": self.col_name, "count": count}
        )
        if res.status_code not in HTTP_OK:
            raise SimpleQueryLastError(res)
        return res.obj["result"]

    def all(self, skip=None, limit=None):
        """Return all documents in this collection.

        ``skip`` is applied before ``limit`` if both are provided.

        :param skip: the number of documents to skip
        :type skip: int
        :param limit: maximum number of documents to return
        :type limit: int
        :returns: the list of all documents
        :rtype: list
        :raises: SimpleQueryAllError
        """
        data = {"collection": self.col_name}
        if skip is not None:
            data["skip"] = skip
        if limit is not None:
            data["limit"] = limit
        res = self.conn.api_put("/simple/all", data=data)
        if res.status_code not in HTTP_OK:
            raise SimpleQueryAllError(res)
        return self.create_cursor(res)

    def any(self):
        """Return a random document from this collection.

        :returns: the random document
        :rtype: dict
        :raises: SimpleQueryAnyError
        """
        res = self.conn.api_put(
            "/simple/any",
            data={"collection": self.col_name}
        )
        if res.status_code not in HTTP_OK:
            raise SimpleQueryAnyError(res)
        return res.obj["document"]

    def get_first_example(self, example):
        """Return the first document matching the given example document body.

        :param example: the example document body
        :type example: dict
        :returns: the first matching document
        :rtype: dict or None
        :raises: SimpleQueryFirstExampleError
        """
        data = {"collection": self.col_name, "example": example}
        res = self.conn.api_put("/simple/first-example", data=data)
        if res.status_code == 404:
            return None
        elif res.status_code not in HTTP_OK:
            raise SimpleQueryFirstExampleError(res)
        return res.obj["document"]

    def get_by_example(self, example, skip=None, limit=None):
        """Return all documents matching the given example document body.

        ``skip`` is applied before ``limit`` if both are provided.

        :param example: the example document body
        :type example: dict
        :param skip: the number of documents to skip
        :type skip: int
        :param limit: maximum number of documents to return
        :type limit: int
        :returns: the list of matching documents
        :rtype: list
        :raises: SimpleQueryGetByExampleError
        """
        data = {"collection": self.col_name, "example": example}
        if skip is not None:
            data["skip"] = skip
        if limit is not None:
            data["limit"] = limit
        res = self.conn.api_put("/simple/by-example", data=data)
        if res.status_code not in HTTP_OK:
            raise SimpleQueryGetByExampleError(res)
        return self.create_cursor(res)

    def update_by_example(self, example, new_value, keep_none=True, limit=None,
                          wait_for_sync=False):
        """Update all documents matching the given example document body.

        :param example: the example document body
        :type example: dict
        :param new_value: the new document body to update with
        :type new_value: dict
        :param keep_none: whether or not to keep the None values
        :type keep_none: bool
        :param limit: maximum number of documents to return
        :type limit: int
        :param wait_for_sync: wait for the update to sync to disk
        :type wait_for_sync: bool
        :returns: the number of documents updated
        :rtype: int
        :raises: SimpleQueryUpdateByExampleError
        """
        data = {
            "collection": self.col_name,
            "example": example,
            "newValue": new_value,
            "keepNull": keep_none,
            "waitForSync": wait_for_sync,
        }
        if limit is not None:
            data["limit"] = limit
        res = self.conn.api_put("/simple/update-by-example", data=data)
        if res.status_code not in HTTP_OK:
            raise SimpleQueryUpdateByExampleError(res)
        return res.obj["updated"]

    def replace_by_example(self, example, new_value, limit=None,
                           wait_for_sync=False):
        """Replace all documents matching the given example.

        ``skip`` is applied before ``limit`` if both are provided.

        :param example: the example document
        :type example: dict
        :param new_value: the new document
        :type new_value: dict
        :param limit: maximum number of documents to return
        :type limit: int
        :param wait_for_sync: wait for the replace to sync to disk
        :type wait_for_sync: bool
        :returns: the number of documents replaced
        :rtype: int
        :raises: SimpleQueryReplaceByExampleError
        """
        data = {
            "collection": self.col_name,
            "example": example,
            "newValue": new_value,
            "waitForSync": wait_for_sync,
        }
        if limit is not None:
            data["limit"] = limit
        res = self.conn.api_put("/simple/replace-by-example", data=data)
        if res.status_code not in HTTP_OK:
            raise SimpleQueryReplaceByExampleError(res)
        return res.obj["replaced"]

    def delete_by_example(self, example, limit=None, wait_for_sync=False):
        """Remove all documents matching the given example.

        :param example: the example document
        :type example: dict
        :param limit: maximum number of documents to return
        :type limit: int
        :param wait_for_sync: wait for the remove to sync to disk
        :type wait_for_sync: bool
        :returns: the number of documents remove
        :rtype: int
        :raises: SimpleQueryRemoveByExampleError
        """
        data = {
            "collection": self.col_name,
            "example": example,
            "waitForSync": wait_for_sync,
        }
        if limit is not None:
            data["limit"] = limit
        res = self.conn.api_put("/simple/remove-by-example", data=data)
        if res.status_code not in HTTP_OK:
            raise SimpleQueryRemoveByExampleError(res)
        return res.obj["deleted"]

    def range(self, attribute, left, right, closed=True, skip=None, limit=None):
        """Return all the documents within a given range.

        In order to execute this query a skiplist index must be present on the
        queried attribute.

        :param attribute: the attribute path with a skip-list index
        :type attribute: str
        :param left: the lower bound
        :type left: int
        :param right: the upper bound
        :type right: int
        :param closed: whether or not to include left and right, or just left
        :type closed: bool
        :param skip: the number of documents to skip
        :type skip: int
        :param limit: maximum number of documents to return
        :type limit: int
        :returns: the list of documents
        :rtype: list
        :raises: SimpleQueryRangeError
        """
        data = {
            "collection": self.col_name,
            "attribute": attribute,
            "left": left,
            "right": right,
            "closed": closed
        }
        if skip is not None:
            data["skip"] = skip
        if limit is not None:
            data["limit"] = limit
        res = self.conn.api_put("/simple/range", data=data)
        if res.status_code not in HTTP_OK:
            raise SimpleQueryRangeError(res)
        return self.create_cursor(res)

    def near(self, latitude, longitude, distance=None, radius=None, skip=None,
             limit=None, geo=None):
        """Return all the documents near the given coordinate.

        By default number of documents returned is 100. The returned list is
        sorted based on the distance, with the nearest document being the first
        in the list. Documents of equal distance are ordered randomly.

        In order to execute this query a geo index must be defined for the
        collection. If there are more than one geo-spatial index, the ``geo``
        argument can be used to select a particular index.

        if ``distance`` is given, return the distance (in meters) to the
        coordinate in a new attribute whose key is the value of the argument.

        :param latitude: the latitude of the coordinate
        :type latitude: int
        :param longitude: the longitude of the coordinate
        :type longitude: int
        :param distance: return the distance to the coordinate in this key
        :type distance: str
        :param skip: the number of documents to skip
        :type skip: int
        :param limit: maximum number of documents to return
        :type limit: int
        :param geo: the identifier of the geo-index to use
        :type geo: str
        :returns: the list of documents that are near the coordinate
        :rtype: list
        :raises: SimpleQueryNearError
        """
        data = {
            "collection": self.col_name,
            "latitude": latitude,
            "longitude": longitude
        }
        if distance is not None:
            data["distance"] = distance
        if radius is not None:
            data["radius"] = radius
        if skip is not None:
            data["skip"] = skip
        if limit is not None:
            data["limit"] = limit
        if geo is not None:
            data["geo"] = geo

        res = self.conn.api_put("/simple/near", data=data)
        if res.status_code not in HTTP_OK:
            raise SimpleQueryNearError(res)
        return self.create_cursor(res)

    # TODO this endpoint does not seem to work
    # Come back to this and check that it works in the next version of ArangoDB
    def within(self, latitude, longitude, radius, distance=None, skip=None,
               limit=None, geo=None):
        """Return all documents within the radius around the coordinate.

        The returned list is sorted by distance from the coordinate. In order
        to execute this query a geo index must be defined for the collection.
        If there are more than one geo-spatial index, the ``geo`` argument can
        be used to select a particular index.

        if ``distance`` is given, return the distance (in meters) to the
        coordinate in a new attribute whose key is the value of the argument.

        :param latitude: the latitude of the coordinate
        :type latitude: int
        :param longitude: the longitude of the coordinate
        :type longitude: int
        :param radius: the maximum radius (in meters)
        :type radius: int
        :param distance: return the distance to the coordinate in this key
        :type distance: str
        :param skip: the number of documents to skip
        :type skip: int
        :param limit: maximum number of documents to return
        :type limit: int
        :param geo: the identifier of the geo-index to use
        :type geo: str
        :returns: the list of documents are within the radius
        :rtype: list
        :raises: SimpleQueryWithinError
        """
        data = {
            "collection": self.col_name,
            "latitude": latitude,
            "longitude": longitude,
            "radius": radius
        }
        if distance is not None:
            data["distance"] = distance
        if skip is not None:
            data["skip"] = skip
        if limit is not None:
            data["limit"] = limit
        if geo is not None:
            data["geo"] = geo

        res = self.conn.api_put("/simple/within", data=data)
        if res.status_code not in HTTP_OK:
            raise SimpleQueryWithinError(res)
        return self.create_cursor(res)

    def fulltext(self, attribute, query, skip=None, limit=None, index=None):
        """Return all documents that match the specified fulltext ``query``.

        In order to execute this query a fulltext index must be defined for the
        collection and the specified attribute.

        For more information on fulltext queries please refer to:
        https://docs.arangodb.com/SimpleQueries/FulltextQueries.html

        :param attribute: the attribute path with a fulltext index
        :type attribute: str
        :param query: the fulltext query
        :type query: str
        :param skip: the number of documents to skip
        :type skip: int
        :param limit: maximum number of documents to return
        :type limit: int
        :returns: the list of documents
        :rtype: list
        :raises: SimpleQueryFullTextError
        """
        data = {
            "collection": self.col_name,
            "attribute": attribute,
            "query": query,
        }
        if skip is not None:
            data["skip"] = skip
        if limit is not None:
            data["limit"] = limit
        if index is not None:
            data["index"] = index
        res = self.conn.api_put("/simple/fulltext", data=data)
        if res.status_code not in HTTP_OK:
            raise SimpleQueryFullTextError(res)
        return self.create_cursor(res)

    def lookup_by_keys(self, keys):
        """Return all documents whose key is in ``keys``.

        :param keys: keys of documents to lookup
        :type keys: list
        :returns: the list of documents
        :rtype: list
        :raises: SimpleQueryLookupByKeysError
        """
        data = {
            "collection": self.col_name,
            "keys": keys,
        }
        res = self.conn.api_put("/simple/lookup-by-keys", data=data)
        if res.status_code not in HTTP_OK:
            raise SimpleQueryLookupByKeysError(res)
        return res.obj["documents"]

    def delete_by_keys(self, keys):
        """Remove all documents whose key is in ``keys``.

        :param keys: keys of documents to remove
        :type keys: list
        :returns: the number of documents removed
        :rtype: dict
        :raises: SimpleQueryRemoveByKeysError
        """
        data = {
            "collection": self.col_name,
            "keys": keys,
        }
        res = self.conn.api_put("/simple/remove-by-keys", data=data)
        if res.status_code not in HTTP_OK:
            raise SimpleQueryRemoveByKeysError(res)
        return {
            "removed": res.obj["removed"],
            "ignored": res.obj["ignored"],
        }
