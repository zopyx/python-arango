"""ArangoDB's AQL function API."""

from arango.constants import HTTP_OK
from arango.api import DatabaseSpecificAPI
from arango.exceptions import (
    AQLFunctionListError,
    AQLFunctionCreateError,
    AQLFunctionDeleteError,
)


class AQLFunctionAPI(DatabaseSpecificAPI):
    """A wrapper around ArangoDB's AQL function API."""

    def __init__(self, connection):
        super(AQLFunctionAPI, self).__init__(connection)

    def list_aql_functions(self):
        """Return the AQL functions defined in this database.

        :returns: a mapping of AQL function names to its javascript code
        :rtype: dict
        :raises: AQLFunctionListError
        """
        res = self.conn.api_get("/aqlfunction")
        if res.status_code not in HTTP_OK:
            raise AQLFunctionListError(res)
        return {func["name"]: func["code"]for func in res.obj}

    def create_aql_function(self, name, code):
        """Add a new AQL function.

        :param name: the name of the new AQL function to add
        :type name: str
        :param code: the stringified javascript code of the new function
        :type code: str
        :returns: the updated AQL functions
        :rtype: dict
        :raises: AQLFunctionCreateError
        """
        data = {"name": name, "code": code}
        res = self.conn.api_post("/aqlfunction", data=data)
        if res.status_code not in HTTP_OK:
            raise AQLFunctionCreateError(res)
        return res.obj

    def delete_aql_function(self, name, group=None):
        """Delete an existing AQL function.

        If ``group`` is set to True, then the function name provided in
        ``name`` is treated as a namespace prefix, and all functions in
        the specified namespace will be deleted. If set to False, the
        function name provided in ``name`` must be fully qualified,
        including any namespaces.

        :param name: the name of the AQL function to delete
        :type name: str
        :param group: whether or not to treat name as a namespace prefix
        :returns: the updated AQL functions
        :rtype: dict
        :raises: AQLFunctionDeleteError
        """
        res = self.conn.api_delete(
            "/aqlfunction/{}".format(name),
            params={"group": group} if group is not None else {}
        )
        if res.status_code not in HTTP_OK:
            raise AQLFunctionDeleteError(res)
        return res.obj
