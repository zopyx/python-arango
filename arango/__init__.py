"""ArangoDB's Top Level API."""

from arango.connection import ArangoConnection
from arango.api.general import (
    DatabaseManagementAPI,
    VersionAPI,
    CursorAPI,
    UserAPI
)


class Arango(DatabaseManagementAPI, VersionAPI, CursorAPI, UserAPI):
    """A wrapper around ArangoDB's general (top-level) API."""

    def __init__(self, arango_connection=None):
        if arango_connection is None:
            arango_connection = ArangoConnection()
        super(Arango, self).__init__(arango_connection)
        # Cache for Database objects
        self._database_cache = {}
        # Default database (i.e. "_system")
        self._default_database = Database(self.conn, "_system")

    def _invalidate_database_cache(self):
        """Invalidate the Database object cache."""
        real_dbs = set(self.databases["all"])
        cached_dbs = set(self._database_cache)
        for db_name in cached_dbs - real_dbs:
            del self._database_cache[db_name]
        for db_name in real_dbs - cached_dbs:
            self._database_cache[db_name] = Database(self.conn, db_name)

    def database(self, name):
        """Return the ``Database`` object of the specified name.

        :returns: the database object
        :rtype: arango.database.Database
        :raises: DatabaseNotFoundError
        """
        if name in self._database_cache:
            return self._database_cache[name]
        else:
            self._invalidate_database_cache()
            if name not in self._database_cache:
                raise DatabaseNotFoundError(name)
            return self._database_cache[name]

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
        self._invalidate_database_cache()
        return self.database(name)

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
        self._invalidate_database_cache()




if __name__ == "__main__":
    a = Arango()
    print(a.version)
