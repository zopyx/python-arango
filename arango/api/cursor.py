"""ArangoDB's cursor API."""

from arango.api import GeneralAPI
from arango.exceptions import (
    AQLQueryExecuteError,
    CursorDeleteError,
)


class CursorAPI(GeneralAPI):
    """A wrapper around ArangoDB's cursor API."""

    def __init__(self, connection):
        super(CursorAPI, self).__init__(connection)

    def create_cursor(self, response):
        """Continuously read from the cursor and yield the result.

        :param response: ArangoDB response object
        :type response: arango.response.ArangoResponse
        :returns: a generator object
        :rtype: generator
        :raises: CursorExecuteError, CursorDeleteError
        """
        for item in response.obj["result"]:
            yield item
        cursor_id = None
        while response.obj["hasMore"]:
            if cursor_id is None:
                cursor_id = response.obj["id"]
            response = self.conn.api_put("/cursor/{}".format(cursor_id))
            if response.status_code != 200:
                raise AQLQueryExecuteError(response)
            for item in response.obj["result"]:
                yield item
        if cursor_id is not None:
            response = self.conn.api_delete("/cursor/{}".format(cursor_id))
            if response.status_code not in {404, 202}:
                raise CursorDeleteError(response)
