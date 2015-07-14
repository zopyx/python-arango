"""ArangoDB's collection API."""

from arango.api import DatabaseSpecificAPI
from arango.constants import HTTP_OK, COLLECTION_STATUSES
from arango.exceptions import (
    CollectionListError,
    CollectionGetError,
    CollectionCreateError,
    CollectionDeleteError,
    CollectionRenameError,
)
from arango.utils import uncamelify


class CollectionAPI(DatabaseSpecificAPI):
    """A wrapper around ArangoDB's collection API."""

    def __init__(self, connection):
        super(CollectionAPI, self).__init__(connection)

    def list_collections(self):
        """Return the names of the collections in this database.

        :returns: the names of the collections
        :rtype: dict
        :raises: CollectionListError
        """
        res = self.conn.api_get("/collection")
        if res.status_code not in HTTP_OK:
            raise CollectionListError(res)

        user_collections = []
        system_collections = []
        for collection in res.obj["collections"]:
            if collection["isSystem"]:
                system_collections.append(collection["name"])
            else:
                user_collections.append(collection["name"])
        return {
            "user": user_collections,
            "system": system_collections,
            "all": user_collections + system_collections,
        }

    def get_collection_info(self, name):
        """Return the properties of this collection.

        :returns: the collection's id, status, key_options etc.
        :rtype: dict
        :raises: CollectionPropertyError
        """
        res = self.conn.api_get("/collection/{}/properties".format(name))
        if res.status_code not in HTTP_OK:
            raise CollectionGetError(res)
        return {
            "id": res.obj["id"],
            "name": res.obj["name"],
            "is_edge": res.obj["type"] == 3,
            "status": COLLECTION_STATUSES.get(
                res.obj["status"],
                "corrupted ({})".format(res.obj["status"])
            ),
            "do_compact": res.obj["doCompact"],
            "is_system": res.obj["isSystem"],
            "is_volatile": res.obj["isVolatile"],
            "journal_size": res.obj["journalSize"],
            "wait_for_sync": res.obj["waitForSync"],
            "key_options": uncamelify(res.obj["keyOptions"])
        }

    def create_collection(self, name, wait_for_sync=False, do_compact=True,
                          journal_size=None, is_system=False, is_volatile=False,
                          key_generator_type="traditional", is_edge=False,
                          allow_user_keys=True, key_increment=None,
                          key_offset=None, number_of_shards=None,
                          shard_keys=None):
        """Add a new collection to this database.

        :param name: name of the new collection
        :type name: str
        :param wait_for_sync: whether or not to wait for sync to disk
        :type wait_for_sync: bool
        :param do_compact: whether or not the collection is compacted
        :type do_compact: bool
        :param journal_size: the max size of the journal or datafile
        :type journal_size: int
        :param is_system: whether or not the collection is a system collection
        :type is_system: bool
        :param is_volatile: whether or not the collection is in-memory only
        :type is_volatile: bool
        :param key_generator_type: ``traditional`` or ``autoincrement``
        :type key_generator_type: str
        :param allow_user_keys: whether or not to allow users to supply keys
        :type allow_user_keys: bool
        :param key_increment: increment value for ``autoincrement`` generator
        :type key_increment: int
        :param key_offset: initial offset value for ``autoincrement`` generator
        :type key_offset: int
        :param is_edge: whether or not the collection is an edge collection
        :type is_edge: bool
        :param number_of_shards: the number of shards to create
        :type number_of_shards: int
        :param shard_keys: the attribute(s) used to determine the target shard
        :type shard_keys: list
        :raises: CollectionCreateError
        """
        key_options = {
            "type": key_generator_type,
            "allowUserKeys": allow_user_keys
        }
        if key_increment is not None:
            key_options["increment"] = key_increment
        if key_offset is not None:
            key_options["offset"] = key_offset
        data = {
            "name": name,
            "waitForSync": wait_for_sync,
            "doCompact": do_compact,
            "isSystem": is_system,
            "isVolatile": is_volatile,
            "type": 3 if is_edge else 2,
            "keyOptions": key_options
        }
        if journal_size is not None:
            data["journalSize"] = journal_size
        if number_of_shards is not None:
            data["numberOfShards"] = number_of_shards
        if shard_keys is not None:
            data["shardKeys"] = shard_keys

        res = self.conn.api_post("/collection", data=data)
        if res.status_code not in HTTP_OK:
            raise CollectionCreateError(res)
        return res.obj

    def delete_collection(self, name):
        """Delete the specified collection from this database.

        :param name: the name of the collection to delete
        :type name: str
        :raises: CollectionDeleteError
        """
        res = self.conn.api_delete("/collection/{}".format(name))
        if res.status_code not in HTTP_OK:
            raise CollectionDeleteError(res)
        return res.obj

    def rename_collection(self, name, new_name):
        """Rename the specified collection in this database.

        :param name: the name of the collection to rename
        :type name: str
        :param new_name: the new name for the collection
        :type new_name: str
        :raises: CollectionRenameError
        """
        res = self.conn.api_put(
            "/collection/{}/rename".format(name),
            data={"name": new_name}
        )
        if res.status_code not in HTTP_OK:
            raise CollectionRenameError(res)
        return res.obj
