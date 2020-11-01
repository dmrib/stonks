"""
Database interaction layer.
"""

import os
import psycopg2
import typing


def get_cursor() -> tuple:
    """
    Create connection to PostgreSQL database and provides query cursor.

    Returns:
        connection and cursor to PostgreSQL database instance.
    """
    # get connection parameters
    database = os.environ.get('DATABASE_NAME')
    user = os.environ.get('POSTGRE_USER')
    password = os.environ.get('POSTGRE_PASSWORD')

    # open connection to PostgreSQL
    connection = psycopg2.connect(
        f'host=127.0.0.1 dbname={database} user={user} password={password}'
    )
    connection.set_session(autocommit=True)

    return connection.cursor(), connection


def run_queries(queries: typing.List[str]) -> None:
    """
    Synchronously execute list of queries on PostgreSQL.

    Args:
        query: list of query strings to be executed
    Returns:
        nothing.
    """
    # get database connection and cursor
    cursor, connection = get_cursor()

    # execute queries
    for query in queries:
        cursor.execute(query)

    # close database connection
    connection.close()


def load_data(file: str, table: str, columns: typing.List[str]) -> None:
    """
    Loads CSV files in batches to PostgreSQL table.

    Args:
        file: path to source CSV file
        table: name of destination table
        columns: list of columns to be derived in order from source file
    Returns:
        nothing.
    """
    # get database connection and cursor
    cursor, connection = get_cursor()

    # copy data into table
    cursor.copy_from(file, table, columns=columns, sep=',')

    # close database connection
    connection.close()
