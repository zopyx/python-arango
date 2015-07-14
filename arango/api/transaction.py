"""ArangoDB's transaction API."""

from arango.api import DatabaseSpecificAPI
from arango.exceptions import TransactionError


class TransactionAPI(DatabaseSpecificAPI):
    """A wrapper around ArangoDB's transaction API."""

    def __init__(self, connection):
        super(TransactionAPI, self).__init__(connection)

    def execute_transaction(self, action, read_collections=None,
                            write_collections=None, params=None,
                            wait_for_sync=False, lock_timeout=None):
        """Execute the transaction and return the result.

        Setting the ``lock_timeout`` to 0 will make ArangoDB not time out
        waiting for a lock.

        :param action: the javascript commands to be executed
        :type action: str
        :param read_collections: the collections read
        :type read_collections: str or list or None
        :param write_collections: the collections written to
        :type write_collections: str or list or None
        :param params: Parameters for the function in action
        :type params: list or dict or None
        :param wait_for_sync: wait for the transaction to sync to disk
        :type wait_for_sync: bool
        :param lock_timeout: timeout for waiting on collection locks
        :type lock_timeout: int or None
        :returns: the results of the execution
        :rtype: dict
        :raises: TransactionError
        """
        data = {"collections": {}, "action": action}
        if read_collections is not None:
            data["collections"]["read"] = read_collections
        if write_collections is not None:
            data["collections"]["write"] = write_collections
        if params is not None:
            data["params"] = params
        http_params = {
            "waitForSync": wait_for_sync,
            "lockTimeout": lock_timeout,
        }
        res = self.conn.api_post("/transaction", data=data, params=http_params)
        if res.status_code != 200:
            raise TransactionError(res)
        return res.obj["result"]
