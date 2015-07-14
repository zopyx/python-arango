"""Tests for managing ArangoDB databases."""

import unittest

from arango import Arango
from arango.utils import is_string
from arango.tests.utils import (
    get_next_db_name
)


class DatabaseManagementTest(unittest.TestCase):
    """Tests for managing ArangoDB databases."""

    def setUp(self):
        self.arango = Arango()
        self.db_name = get_next_db_name(self.arango)

        # Test database cleaup
        self.addCleanup(self.arango.delete_database,
                        name=self.db_name, safe_delete=True)

    def test_database_create_and_delete(self):
        self.arango.create_database(self.db_name)
        self.assertIn(self.db_name, self.arango.databases["all"])

        # Check the properties of the new database
        self.assertEqual(self.arango.database(self.db_name).db_name,
                         self.db_name)
        self.assertEqual(self.arango.database(self.db_name).is_system, False)

        # Remove the test database
        self.arango.delete_database(self.db_name)
        self.assertNotIn(self.db_name, self.arango.databases["all"])

    def test_database_properties(self):
        db = self.arango.database("_system")
        self.assertEqual(db.db_name, "_system")
        self.assertTrue(isinstance(db.properties, dict))
        self.assertTrue(is_string(db.id))
        self.assertTrue(is_string(db.path))
        self.assertEqual(db.is_system, True)


if __name__ == "__main__":
    unittest.main()
