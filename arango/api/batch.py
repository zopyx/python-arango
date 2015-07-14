"""ArangoDB's batch API."""

import json
import inspect
try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode
from arango.exceptions import (
    BatchInvalidError,
    BatchExecuteError
)
from arango.constants import HTTP_OK
from arango.api import DatabaseSpecificAPI


class BatchAPI(DatabaseSpecificAPI):
    """A wrapper around ArangoDB's batch API."""

    def __init__(self, connection):
        super(BatchAPI, self).__init__(connection)

    def execute_batch(self, requests):
        data = ""
        for content_id, request in enumerate(requests, start=1):
            try:
                func, args, kwargs = request
            except (TypeError, ValueError):
                raise BatchInvalidError(
                    "pos {}: malformed request".format(content_id)
                )
            if "_batch" not in inspect.getargspec(func)[0]:
                raise BatchInvalidError(
                    "pos {}: ArangoDB method '{}' does not support "
                    "batch execution".format(content_id, func.__name__)
                )
            kwargs["_batch"] = True
            res = func(*args, **kwargs)
            data += "--XXXsubpartXXX\r\n"
            data += "Content-Type: application/x-arango-batchpart\r\n"
            data += "Content-Id: {}\r\n\r\n".format(content_id)
            data += "{}\r\n".format(stringify_request(**res))
        data += "--XXXsubpartXXX--\r\n\r\n"
        res = self.conn.api_post(
            "/batch",
            headers={
                "Content-Type": "multipart/form-data; boundary=XXXsubpartXXX"
            },
            data=data,
        )
        if res.status_code not in HTTP_OK:
            raise BatchExecuteError(res)
        if res.obj is None:
            return []
        else:
            return [
                json.loads(string) for string in res.obj.split("\r\n") if
                string.startswith("{") and string.endswith("}")
            ]


def stringify_request(method, path, params=None, headers=None, data=None):
    path += "?" + urlencode(params) if params else path
    request_string = "{} {} HTTP/1.1".format(method, path)
    if headers:
        for key, value in headers.items():
            request_string += "\r\n{key}: {value}".format(
                key=key, value=value
            )
    if data:
        request_string += "\r\n\r\n{}".format(json.dumps(data))
    return request_string
