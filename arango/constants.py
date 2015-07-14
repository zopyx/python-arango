"""ArangoDB constants."""

# Name of the default ArangoDB database
DEFAULT_DATABASE_NAME = "_system"

# Valid collection types
COLLECTION_TYPES = {"document", "edge"}

# Valid collection statuses
COLLECTION_STATUSES = {
    1: "new",
    2: "unloaded",
    3: "loaded",
    4: "unloading",
    5: "deleted",
}

# 'HTTP OK' status codes
HTTP_OK = {
    200, "200",
    201, "201",
    202, "202",
    203, "203",
    204, "204",
    205, "205",
    206, "206",
}
