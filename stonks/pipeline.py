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
        print('\n🔥 Destroying existing resources... \n')
        run_queries(TEARDOWN)

    # initialize database tables
    print('\n🔨 Initializing database...\n')
    run_queries(INITIALIZE)

    # extract and unload data from remote API
    print('\n✂️ Extracting source data...\n')
    for currency in tqdm.tqdm(CURRENCIES):
        rates = fetch_year_exchange_rates(currency, '2020')
        unload_exchange_rates('./data/', rates)

    # load data into final tables
    print('\n📦 Loading tables...\n')
    for currency in tqdm.tqdm(CURRENCIES):
        with open(f'./data/currencies-{currency}.csv', 'r') as input_file:
            load_data(
                input_file,
                'currencies.fact_exchange_rate',
                columns = ['currency_source', 'currency_date', *CURRENCIES]
            )
    run_queries(TRANSFORMATIONS)


# execute pipeline
run(True)
