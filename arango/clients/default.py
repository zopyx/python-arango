"""Default ArangoDB client."""

import requests

from arango.response import ArangoResponse
from arango.clients.base import BaseClient


class DefaultClient(BaseClient):
    """Default client for ArangoDB connection (arango.connection.Connection).

    This is a session-based client using the Python library ``requests``.

    :param username: username of the ArangoDB user
    :type username: str or None
    :param password: password of the ArangoDB user
    :type password: str or None
    """

    def __init__(self, username=None, password=None):
        self.session = requests.Session()
        self.session.auth = (username, password)

    def head(self, url, params=None, headers=None, auth=None):
        """HTTP HEAD method.

        :param url: request URL
        :type url: str
        :param params: request parameters
        :type params: dict or None
        :param headers: request headers
        :type headers: dict or None
        :param auth: username and password tuple
        :type auth: tuple or None
        :returns: ArangoDB http response object
        :rtype: arango.response.ArangoResponse
        """
        res = self.session.head(
            url=url,
            params=params,
            headers=headers,
        )
        return ArangoResponse(
            method="head",
            url=url,
            status_code=res.status_code,
            content=res.text,
            status_text=res.reason
        )

    def get(self, url, params=None, headers=None, auth=None):
        """HTTP GET method.

        :param url: request URL
        :type url: str
        :param params: request parameters
        :type params: dict or None
        :param headers: request headers
        :type headers: dict or None
        :param auth: username and password tuple
        :type auth: tuple or None
        :returns: ArangoDB http response object
        :rtype: arango.response.ArangoResponse
        """
        res = self.session.get(
            url=url,
            params=params,
            headers=headers,
        )
        return ArangoResponse(
            method="get",
            url=url,
            status_code=res.status_code,
            content=res.text,
            status_text=res.reason
        )

    def put(self, url, data=None, params=None, headers=None, auth=None):
        """HTTP PUT method.

        :param url: request URL
        :type url: str
        :param data: request payload
        :type data: str or dict or None
        :param params: request parameters
        :type params: dict or None
        :param headers: request headers
        :type headers: dict or None
        :param auth: username and password tuple
        :type auth: tuple or None
        :returns: ArangoDB http response object
        :rtype: arango.response.ArangoResponse
        """
        res = self.session.put(
            url=url,
            data=data,
            params=params,
            headers=headers,
        )
        return ArangoResponse(
            method="put",
            url=url,
            status_code=res.status_code,
            content=res.text,
            status_text=res.reason
        )

    def post(self, url, data=None, params=None, headers=None, auth=None):
        """HTTP POST method.

        :param url: request URL
        :type url: str
        :param data: request payload
        :type data: str or dict or None
        :param params: request parameters
        :type params: dict or None
        :param headers: request headers
        :type headers: dict or None
        :param auth: username and password tuple
        :type auth: tuple or None
        :returns: ArangoDB http response object
        :rtype: arango.response.ArangoResponse
        """
        res = self.session.post(
            url=url,
            data="" if data is None else data,
            params={} if params is None else params,
            headers={} if headers is None else headers,
        )
        return ArangoResponse(
            method="post",
            url=url,
            status_code=res.status_code,
            content=res.text,
            status_text=res.reason
        )

    def patch(self, url, data=None, params=None, headers=None, auth=None):
        """HTTP PATCH method.

        :param url: request URL
        :type url: str
        :param data: request payload
        :type data: str or dict or None
        :param params: request parameters
        :type params: dict or None
        :param headers: request headers
        :type headers: dict or None
        :param auth: username and password tuple
        :type auth: tuple or None
        :returns: ArangoDB http response object
        :rtype: arango.response.ArangoResponse
        """
        res = self.session.patch(
            url=url,
            data=data,
            params=params,
            headers=headers,
        )
        return ArangoResponse(
            method="patch",
            url=url,
            status_code=res.status_code,
            content=res.text,
            status_text=res.reason
        )

    def delete(self, url, params=None, headers=None, auth=None):
        """HTTP DELETE method.

        :param url: request URL
        :type url: str
        :param params: request parameters
        :type params: dict or None
        :param headers: request headers
        :type headers: dict or None
        :param auth: username and password tuple
        :type auth: tuple or None
        :returns: ArangoDB http response object
        :rtype: arango.response.ArangoResponse
        """
        res = self.session.delete(
            url=url,
            params=params,
            headers=headers,
        )
        return ArangoResponse(
            method="delete",
            url=url,
            status_code=res.status_code,
            content=res.text,
            status_text=res.reason
        )
