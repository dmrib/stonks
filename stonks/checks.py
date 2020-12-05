"""
Loaded data sanity checks.
"""

from database import get_values

import tqdm
import typing


def check_for_minimum_rows(query: str,
                           min_rows: int,
                           tables: typing.List[str]) -> None:
    """
    Check whether query returns minimum amount of rows.

    Args:
        query:    query to be executed
        min_rows: minimum amount of rows returned
        tables:   tables to have integrity checked

    Returns:
        nothing.
    """
    print('\n😱 Checking tables integrity... \n')

    # check tables integrity
    for table in tqdm.tqdm(tables):

        # execute query
        results = get_values(query.format(table=table, min=min_rows))

        # minimim result number not reached: fail
        if len(results) < min_rows:
            raise AssertionError(f"😔 Minimum row values not met for {table}")


def check_static_file_is_fully_loaded(query: str,
                                      rows_number: int,
                                      tables: typing.List[str]) -> None:
    """
    Check whether static source files were fully loaded.

    Args:
        query:       check query
        rows_number: expected returned rows number
        tables:      tables to have row number checked

    Returns:
        nothing.
    """
    print('\n😱 Checking static files were loaded... \n')

    # check tables integrity
    for table in tqdm.tqdm(tables):

        # execute query
        results = get_values(query.format(table=table))

        # rows number does not match: fail
        if len(results) != rows_number:
            raise AssertionError(f"😔 Minimum row values not met for {table}")
