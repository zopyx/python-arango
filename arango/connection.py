"""ArangoDB API connection."""

import json

from arango.utils import is_string
from arango.constants import HTTP_OK, DEFAULT_DATABASE_NAME
from arango.clients.default import DefaultClient
from arango.exceptions import ArangoConnectionError


class ArangoConnection(object):
    """Makes HTTP REST API calls to ArangoDB using the given client.

    If ``client`` is not specified a default client using the ``requests``
    Python library is used.

    Each API call method in this class takes a database name as its argument,
    due to the fact that any operation triggered via ArangoDB's API is executed
    in context of exactly one database.

    :param protocol: internet transfer protocol (default: 'http')
    :type protocol: str
    :param host: ArangoDB host (default: 'localhost')
    :type host: str
    :param port: ArangoDB port (default: 8529)
    :type port: int
    :param username: username of the ArangoDB user (default: 'root')
    :type username: str
    :param password: password of the ArangoDB user (default: '')
    :type password: str
    :param client: HTTP client for the connection to use
    :type client: arango.clients.base.BaseClient or None
    :raises: ArangoConnectionError
    """

    def __init__(self, protocol="http", host="localhost", port=8529,
                 username="root", password="", client=None):
        self.protocol = protocol
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = DefaultClient() if client is None else client

        # Check the connection by requesting a header
        res = self.api_head(DEFAULT_DATABASE_NAME, "/version")
        if res.status_code not in HTTP_OK:
            raise ArangoConnectionError(res)

    def url_prefix(self, database_name):
        """Generate and return the URL prefix with ``database_name``.

        :param database_name: name of the database to include in the prefix
        :type database_name: str
        :returns: URL prefix
        :rtype: str
        """
        return "{protocol}://{host}:{port}/_db/{db}/_api".format(
            protocol=self.protocol,
            host=self.host,
            port=self.port,
            db=database_name
        )

    def api_head(self, database_name, api_path, params=None, headers=None):
        """Execute ArangoDB's API HEAD method.

        :param database_name: name of the database to make the API call to
        :type database_name: str
        :param api_path: API path (e.g. '/_api/version')
        :type api_path: str
        :param params: request parameters
        :type params: dict or None
        :param headers: request headers
        :type headers: dict or None
        :returns: ArangoDB http response
        :rtype: arango.response.ArangoResponse
        """
        return self.client.head(
            url=self.url_prefix(database_name) + api_path,
            params=params,
            headers=headers,
            auth=(self.username, self.password),
        )

    def api_get(self, database_name, api_path, params=None, headers=None):
        """Execute ArangoDB's API GET method.

        :param database_name: name of the database to make the API call to
        :type database_name: str
        :param api_path: API path (e.g. '/_api/version')
        :type api_path: str
        :param params: request parameters
        :type params: dict or None
        :param headers: request headers
        :type headers: dict or None
        :returns: ArangoDB http response
        :rtype: arango.response.ArangoResponse
        """
        return self.client.get(
            url=self.url_prefix(database_name) + api_path,
            params=params,
            headers=headers,
            auth=(self.username, self.password),
        )

    def api_put(self, database_name, api_path, data=None, params=None,
                headers=None):
        """Execute ArangoDB's API PUT method.

        :param database_name: name of the database to make the API calls to
        :type database_name: str
        :param api_path: API path (e.g. '/_api/version')
        :type api_path: str
        :param data: request payload
        :type data: str or dict or None
        :param params: request parameters
        :type params: dict or None
        :param headers: request headers
        :type headers: dict or None
        :returns: ArangoDB http response
        :rtype: arango.response.ArangoResponse
        """
        return self.client.put(
            url=self.url_prefix(database_name) + api_path,
            data=data if is_string(data) else json.dumps(data),
            params=params,
            headers=headers,
            auth=(self.username, self.password),
        )

    def api_post(self, database_name, api_path, data=None, params=None,
                 headers=None):
        """Execute ArangoDB's API POST method.

        :param database_name: name of the database to make the API calls to
        :type database_name: str
        :param api_path: API path (e.g. '/_api/version')
        :type api_path: str
        :param data: request payload
        :type data: str or dict or None
        :param params: request parameters
        :type params: dict or None
        :param headers: request headers
        :type headers: dict or None
        :returns: ArangoDB http response
        :rtype: arango.response.ArangoResponse
        """
        return self.client.post(
            url=self.url_prefix(database_name) + api_path,
            data=data if is_string(data) else json.dumps(data),
            params=params,
            headers=headers,
            auth=(self.username, self.password),
        )

    def api_patch(self, database_name, api_path, data=None, params=None,
                  headers=None):
        """Execute ArangoDB's HTTP PATCH method.

        :param database_name: name of the database to make the API calls to
        :type database_name: str
        :param api_path: API path (e.g. '/_api/version')
        :type api_path: str
        :param data: request payload
        :type data: str or dict or None
        :param params: request parameters
        :type params: dict or None
        :param headers: request headers
        :type headers: dict or None
        :returns: ArangoDB http response
        :rtype: arango.response.ArangoResponse
        """
        return self.client.patch(
            url=self.url_prefix(database_name) + api_path,
            data=data if is_string(data) else json.dumps(data),
            params=params,
            headers=headers,
            auth=(self.username, self.password),
        )

    def api_delete(self, database_name, api_path, params=None, headers=None):
        """Execute an HTTP DELETE method.

        :param database_name: name of the database to make the API call to
        :type database_name: str
        :param api_path: API path (e.g. '/_api/version')
        :type api_path: str
        :param params: request parameters
        :type params: dict or None
        :param headers: request headers
        :type headers: dict or None
        :returns: ArangoDB http response
        :rtype: arango.response.ArangoResponse
        """
        return self.client.delete(
            url=self.url_prefix(database_name) + api_path,
            params=params,
            headers=headers,
            auth=(self.username, self.password),
        )
