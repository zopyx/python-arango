"""ArangoDB's user API."""

from arango.api import GeneralAPI
from arango.constants import HTTP_OK
from arango.exceptions import (
    UserGetAllError,
    UserGetError,
    UserCreateError,
    UserUpdateError,
    UserReplaceError,
    UserDeleteError
)


class UserAPI(GeneralAPI):
    """A wrapper around ArangoDB's user API."""

    def __init__(self, connection):
        super(UserAPI, self).__init__(connection)

    def get_all_users(self):
        """Return the details on all users.

        :returns: a dictionary mapping user names to their information
        :rtype: dict
        :raises: UserGetAllError
        """
        res = self.conn.api_get("/_api/user")
        if res.status_code not in HTTP_OK:
            raise UserGetAllError(res)
        # Uncamelify key(s) for consistent style
        result = {}
        for record in res.obj["result"]:
            result[record["user"]] = {
                "change_password": record.get("changePassword"),
                "active": record.get("active"),
                "extra": record.get("extra"),
            }
        return result

    def get_user(self, username):
        """Return the information on the ArangoDB user with ``username``.

        :param username: the username of the ArangoDB user
        :param
        """

        res = self.conn.api_get("/user/{user}".format(user=username))
        if res.status_code not in HTTP_OK:
            raise UserGetError(username)
        return {
            "active": res.obj.get("active"),
            "change_password": res.obj.get("changePassword"),
            "extra": res.obj.get("extra"),
        }

    def create_user(self, username, password, active=None, extra=None,
                    change_password=None):
        data = {"user": username, "passwd": password}
        if active is not None:
            data["active"] = active
        if extra is not None:
            data["extra"] = extra
        if change_password is not None:
            data["changePassword"] = change_password

        res = self.conn.api_post("/user", data=data)
        if res.status_code not in HTTP_OK:
            raise UserCreateError(res)
        return {
            "active": res.obj.get("active"),
            "change_password": res.obj.get("changePassword"),
            "extra": res.obj.get("extra"),
        }

    def update_user(self, username, password=None, active=None, extra=None,
                    change_password=None):
        data = {}
        if password is not None:
            data["password"] = password
        if active is not None:
            data["active"] = active
        if extra is not None:
            data["extra"] = extra
        if change_password is not None:
            data["changePassword"] = change_password

        res = self.conn.api_patch(
            "/user/{user}".format(user=username), data=data
        )
        if res.status_code not in HTTP_OK:
            raise UserUpdateError(res)
        return {
            "active": res.obj.get("active"),
            "change_password": res.obj.get("changePassword"),
            "extra": res.obj.get("extra"),
        }

    def replace_user(self, username, password, active=None, extra=None,
                     change_password=None):
        data = {"user": username, "password": password}
        if active is not None:
            data["active"] = active
        if extra is not None:
            data["extra"] = extra
        if change_password is not None:
            data["changePassword"] = change_password

        res = self.conn.api_put(
            "/user/{user}".format(user=username), data=data
        )
        if res.status_code not in HTTP_OK:
            raise UserReplaceError(res)
        return {
            "active": res.obj.get("active"),
            "change_password": res.obj.get("changePassword"),
            "extra": res.obj.get("extra"),
        }

    def delete_user(self, username, safe_delete=False):
        res = self.conn.api_delete("/user/{user}".format(user=username))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and safe_delete):
                raise UserDeleteError(res)
        return True
