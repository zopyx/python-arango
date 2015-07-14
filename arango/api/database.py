"""ArangoDB's database API."""

from arango.api import GeneralAPI
from arango.constants import HTTP_OK
from arango.exceptions import (
    DatabaseListError,
    DatabaseGetError,
    DatabaseCreateError,
    DatabaseDeleteError,
)
from arango.utils import uncamelify


class DatabaseAPI(GeneralAPI):
    """A wrapper around ArangoDB's database API."""

    def __init__(self, connection):
        super(DatabaseAPI, self).__init__(connection)

    def list_databases(self):
        """"Return the database names.

        :returns: the database names
        :rtype: dict
        :raises: DatabaseListError
        """
        res = self.conn.api_get("/database/user")
        if res.status_code not in HTTP_OK:
            raise DatabaseListError(res)
        user_databases = res.obj["result"]

        res = self.conn.api_get("/database")
        if res.status_code not in HTTP_OK:
            raise DatabaseListError(res)
        all_databases = res.obj["result"]

        return {"all": all_databases, "user": user_databases}

    def get_database(self):
        """Return all properties of this database.

        :returns: the database properties
        :rtype: dict
        :raises: DatabasePropertyError
        """
        res = self.conn.api_get("/database/current")
        if res.status_code not in HTTP_OK:
            raise DatabaseGetError(res)
        return uncamelify(res.obj["result"])

    def create_database(self, name, users=None):
        """Add a new database.

        :param name: the name of the new database
        :type name: str
        :param users: the users configurations
        :type users: dict
        :returns: the Database object
        :rtype: arango.database.Database
        :raises: DatabaseCreateError
        """
        data = {"name": name, "users": users} if users else {"name": name}
        res = self.conn.api_post("/database", data=data)
        if res.status_code not in HTTP_OK:
            raise DatabaseCreateError(res)
        return res.obj

    def delete_database(self, name, safe_delete=False):
        """Remove the database of the specified name.

        :param name: the name of the database to delete
        :type name: str
        :param safe_delete: whether to execute a safe delete (ignore 404)
        :type safe_delete: bool
        :raises: DatabaseDeleteError
        """
        res = self.conn.api_delete("/database/{}".format(name))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and safe_delete):
                raise DatabaseDeleteError(res)
        return res.obj
