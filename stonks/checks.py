"""
Loaded data sanity checks.
"""

from database import get_values
from sql_queries import CHECK_FOR_MINIMUM

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
