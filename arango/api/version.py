"""ArangoDB's version API."""

from arango.api import GeneralAPI
from arango.constants import HTTP_OK
from arango.exceptions import VersionGetError


class VersionAPI(GeneralAPI):
    """A wrapper around ArangoDB's version API."""

    def __init__(self, connection):
        super(VersionAPI, self).__init__(connection)

    def get_version(self):
        """Return the version of ArangoDB.

        :returns: the version number
        :rtype: str
        :raises: VersionGetError
        """
        res = self.conn.api_get("/version")
        if res.status_code not in HTTP_OK:
            raise VersionGetError(res)
        return res.obj["version"]
