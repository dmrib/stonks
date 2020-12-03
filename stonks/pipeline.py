"""
ETL pipeline for currency exchange rate dataset.
"""

from database import run_queries
from database import load_data
from data_extraction import CURRENCIES
from data_extraction import fetch_yearly_exchange_rates
from data_extraction import unload_exchange_rates
from sql_queries import TEARDOWN, INITIALIZE, TRANSFORMATIONS
from utils import format_prices_data

import glob
import tqdm


def run(teardown: bool = False, format_price_files: bool = False) -> None:
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

    # extract and unload currency data from remote API
    extract_currencies_source_data()

    # load currency data into final tables
    load_final_currencies_tables()

    # format stocks and ETFs source files flagged: format files
    if format_price_files:
        format_prices_data('./data/stocks')
        format_prices_data('./data/ETFs')

    # load stocks and ETF prices final tables
    load_final_prices_tables('stocks', 'fact_stock_price')
    load_final_prices_tables('ETFs', 'fact_etf_price')

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


def extract_currencies_source_data() -> None:
    """
    Extracts and unloads data from external API.

    Returns:
        nothing.
    """
    print('\n✂️ Extracting currency exchanges source data...\n')
    for currency in tqdm.tqdm(CURRENCIES):
        rates = fetch_yearly_exchange_rates(currency, 1999)
        unload_exchange_rates('./data/currencies/', rates)


def load_final_currencies_tables() -> None:
    """
    Loads final currencies tables into destination database.

    Returns:
        nothing.
    """
    print('\n📦 Loading currency exchange tables...\n')

    # load currency rate facts table
    for currency in tqdm.tqdm(CURRENCIES):
        with open(f'./data/currencies/currencies-{currency}.csv', 'r') as input_file:
            load_data(
                input_file,
                'currencies.fact_exchange_rate',
                columns=['currency_source', 'currency_date', *CURRENCIES]
            )

    # load currency dimensions table
    with open(f'./data/currencies/currencies-meta.csv', 'r') as input_file:
        load_data(
            input_file,
            'currencies.dim_currency',
            columns=['currency_source', 'currency_name','subunit', 'symbol']
        )

    # load date dimensions table
    run_queries(TRANSFORMATIONS)


def load_final_prices_tables(source: str, table: str) -> None:
    """
    Loads final stocks and ETF prices tables.

    Args:
        source: whether load from 'stocks' or 'ETFs' folder
        table:  destination table name

    Returns:
        nothing.
    """
    print(f'\n📦 Loading {source} prices tables...\n')

    # get stocks prices source files paths
    stock_files = glob.glob(f'./data/{source}/*.txt')

    # load stocks prices data to database
    for stock_file in tqdm.tqdm(stock_files):
        with open(stock_file, 'r') as input_file:
            load_data(
                input_file,
                f'currencies.{table}',
                columns=[
                    'stock_symbol',
                    'price_date',
                    'open',
                    'high',
                    'low',
                    'close',
                    'volume'
                ]
            )


if __name__ == '__main__':

    # execute pipeline
    run(True)
