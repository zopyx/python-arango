"""ArangoDB HTTP response."""

import json


class ArangoResponse(object):
    """ArangoDB HTTP Response class.

    The clients in arango.clients MUST return an instance of this class.

    :param method: the HTTP method executed
    :type method: str
    :param url: the requested URL
    :type url: str
    :param status_code: HTTP status code returned
    :type status_code: int
    :param content: HTTP response content returned
    :type content: basestring or str
    :param status_text: HTTP status description returned if any
    :type status_text: str or None
    """

    def __init__(self, method, url, status_code, content, status_text=None):
        self.method = method
        self.url = url
        self.status_code = status_code
        self.status_text = status_text
        try:
            self.obj = json.loads(content) if content else None
        except ValueError:
            self.obj = None
