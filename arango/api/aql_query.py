"""ArangoDB's AQL query API."""

from arango.utils import uncamelify
from arango.constants import HTTP_OK
from arango.api.cursor import CursorAPI
from arango.api import DatabaseSpecificAPI
from arango.exceptions import (
    AQLQueryExplainError,
    AQLQueryExecuteError,
    AQLQueryValidateError,
)


class AQLQueryAPI(DatabaseSpecificAPI, CursorAPI):
    """A wrapper around ArangoDB's AQL query API."""

    def __init__(self, connection):
        super(AQLQueryAPI, self).__init__(connection)

    def explain_query(self, query, all_plans=False, max_plans=None,
                      optimizer_rules=None):
        """Explain the AQL query.

        This method does not execute the query, but only inspect it and
        return meta information about it.

        If ``all_plans`` is set to True, all possible execution plans are
        returned. Otherwise only the optimal plan is returned.

        For more information on optimizer_rules, please refer to:
        https://docs.arangodb.com/HttpAqlQuery/README.html

        :param query: the AQL query to explain
        :type query: str
        :param all_plans: whether or not to return all execution plans
        :type all_plans: bool
        :param max_plans: maximum number of plans the optimizer generates
        :type max_plans: None or int
        :param optimizer_rules: list of optimizer rules
        :type optimizer_rules: list
        :returns: the query plan or list of plans (if all_plans is True)
        :rtype: dict or list
        :raises: AQLQueryExplainError
        """
        options = {"allPlans": all_plans}
        if max_plans is not None:
            options["maxNumberOfPlans"] = max_plans
        if optimizer_rules is not None:
            options["optimizer"] = {"rules": optimizer_rules}
        res = self.conn.api_post(
            "/explain", data={"query": query, "options": options}
        )
        if res.status_code not in HTTP_OK:
            raise AQLQueryExplainError(res)
        if "plan" in res.obj:
            return uncamelify(res.obj["plan"])
        else:
            return uncamelify(res.obj["plans"])

    def validate_query(self, query):
        """Validate the AQL query.

        :param query: the AQL query to validate
        :type query: str
        :raises: AQLQueryValidateError
        """
        res = self.conn.api_post("/query", data={"query": query})
        if res.status_code not in HTTP_OK:
            raise AQLQueryValidateError(res)
        return res.obj

    def execute_query(self, query, count=False, batch_size=None, ttl=None,
                      bind_vars=None, full_count=None, max_plans=None,
                      optimizer_rules=None):
        """Execute the AQL query and return the result.

        For more information on ``full_count`` please refer to:
        https://docs.arangodb.com/HttpAqlQueryCursor/AccessingCursors.html

        :param query: the AQL query to execute
        :type query: str
        :param count: whether or not the document count should be returned
        :type count: bool
        :param batch_size: maximum number of documents in one round trip
        :type batch_size: int
        :param ttl: time-to-live for the cursor (in seconds)
        :type ttl: int
        :param bind_vars: key-value pairs of bind parameters
        :type bind_vars: dict
        :param full_count: whether or not to include count before last LIMIT
        :param max_plans: maximum number of plans the optimizer generates
        :type max_plans: None or int
        :param optimizer_rules: list of optimizer rules
        :type optimizer_rules: list
        :returns: the cursor from executing the query
        :raises: AQLQueryExecuteError, CursorDeleteError
        """
        options = {}
        if full_count is not None:
            options["fullCount"] = full_count
        if max_plans is not None:
            options["maxNumberOfPlans"] = max_plans
        if optimizer_rules is not None:
            options["optimizer"] = {"rules": optimizer_rules}

        data = {
            "query": query,
            "count": count,
        }
        if batch_size is not None:
            data["batchSize"] = batch_size
        if ttl is not None:
            data["ttl"] = ttl
        if bind_vars is not None:
            data["bindVars"] = bind_vars
        if options:
            data["options"] = options

        res = self.conn.api_post("/cursor", data=data)
        if res.status_code not in HTTP_OK:
            raise AQLQueryExecuteError(res)
        return self.create_cursor(res)
