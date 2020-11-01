"""
ETL pipeline for currency exchange rate dataset.
"""

from database import run_queries
from database import load_data
from data_extraction import CURRENCIES
from data_extraction import fetch_year_exchange_rates
from data_extraction import unload_exchange_rates
from sql_queries import TEARDOWN, INITIALIZE, TRANSFORMATIONS

import tqdm


def run(teardown=False) -> None:
    """
    Execute ETL pipeline for currency exchange rate dataset.

    Args:
        teardown: whether existing schema should be dropped
    Returns:
        nothing.
    """
    # teardown is flagged: drop existing schema if exists
    if teardown:
       teardown_database()

    # initialize database tables
    initialize_database()

    # extract and unload data from remote API
    extract_source_data()

    # load data into final tables
    load_final_tables()

    print('\n\n🎉 Done!\n')


def teardown_database() -> None:
    """
    Drop database schema as well as existing tables.

    Returns:
        nothing.
    """
    print('\n🔥 Destroying existing resources... \n')
    run_queries(TEARDOWN)


def initialize_database() -> None:
    """
    Create required database schema and tables.

    Returns:
        nothing.
    """
    print('\n🔨 Initializing database...\n')
    run_queries(INITIALIZE)


def extract_source_data() -> None:
    """
    Extracts and unloads data from external API.

    Returns:
        nothing.
    """
    print('\n✂️ Extracting source data...\n')
    for currency in tqdm.tqdm(CURRENCIES):
        rates = fetch_year_exchange_rates(currency, '2020')
        unload_exchange_rates('./data/', rates)


def load_final_tables() -> None:
    """
    Loads final tables into destination database.

    Returns:
        nothing.
    """
    print('\n📦 Loading tables...\n')

    # load currency rate facts table
    for currency in tqdm.tqdm(CURRENCIES):
        with open(f'./data/currencies-{currency}.csv', 'r') as input_file:
            load_data(
                input_file,
                'currencies.fact_exchange_rate',
                columns=['currency_source', 'currency_date', *CURRENCIES]
            )

    # load currency dimensions table
    with open(f'./data/currencies-meta.csv', 'r') as input_file:
        load_data(
            input_file,
            'currencies.dim_currency',
            columns=['currency_source', 'currency_name','subunit', 'symbol']
        )

    # load date dimensions table
    run_queries(TRANSFORMATIONS)


# execute pipeline
run(True)
