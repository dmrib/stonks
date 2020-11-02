"""
Tests for database layer 'get_cursor' method.
"""

from stonks.database import get_cursor
from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch


CONNECTION_PARAMS = ['A', 'B', 'C']
EXPECTED = 'host=127.0.0.1 dbname=A user=B password=C'


class TestDatabaseGetCursor(TestCase):
    """
    Test case for getting connection and cursor database abstractions.
    """

    def setUp(self):
        """
        Prepares for testing.
        """
        self.connection = MagicMock()
        self.cursor = MagicMock()
        self.connection.cursor.return_value = self.cursor


    @patch('stonks.database.psycopg2.connect')
    @patch('stonks.database.os.environ.get')
    def test_string_connection_is_properly_formatted(self, get, connect):
        """
        Tests whether connection string formatting is being made properly.
        """
        # mock environment variables retrieval
        get.side_effect = CONNECTION_PARAMS

        # get cursor
        get_cursor()

        # formatted?
        connect.assert_called_once_with(EXPECTED)


    @patch('stonks.database.psycopg2.connect')
    def test_autocommit_is_being_set(self, connect):
        """
        Tests whether connection has autocommit enabled.
        """
        # set mocked connection function return value
        connect.return_value = self.connection

        # get cursor
        get_cursor()

        # enabled?
        self.connection.set_session.assert_called_once_with(autocommit=True)


    @patch('stonks.database.psycopg2.connect')
    def test_cursor_and_connection_are_being_returned(self, connect):
        """
        Tests whether cursor and connection are being returned.
        """
        # set mocked connection function return value
        connect.return_value = self.connection

        # get cursor and connection
        cursor, connection = get_cursor()

        # returned?
        self.assertEqual(cursor, self.cursor)
        self.assertEqual(connection, self.connection)
