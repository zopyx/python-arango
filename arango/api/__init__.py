"""Base API classes."""


class GeneralAPI:

    def __init__(self, connection):
        self.conn = connection


class DatabaseSpecificAPI:

    def __init__(self, connection, database_name):
        self.conn = connection
        self.db_name = database_name


class GraphSpecificAPI:

    def __init__(self, connection, database_name, graph_name):
        self.conn = connection
        self.db_name = database_name
        self.graph_name = graph_name


class CollectionSpecificAPI:

    def __init__(self, connection, database_name, collection_name):
        self.conn = connection
        self.db_name = database_name
        self.col_name = collection_name
