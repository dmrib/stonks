"""
ETL pipeline for currency exchange rate dataset.
"""

from checks import check_for_minimum_rows
from database import run_queries
from database import load_data
from extraction import CURRENCIES
from extraction import fetch_yearly_exchange_rates
from extraction import unload_exchange_rates
from sql_queries import TEARDOWN, INITIALIZE, TRANSFORM_DATES, CHECK_FOR_MINIMUM
from formatters import format_commodities_data
from formatters import format_prices_data

import glob
import tqdm


TABLES = [
    'currencies.fact_exchange_rate',
    'currencies.dim_date',
    'currencies.dim_currency',
    'currencies.fact_stock_price',
    'currencies.fact_etf_price',
    'currencies.fact_commodities_stats',
    'currencies.dim_commodity'
]


def run(teardown: bool = False,
        format_price_files: bool = False,
        format_commodities_files: bool = False) -> None:
    """
    Execute ETL pipeline for currency exchange rate dataset.

    Args:
        teardown: whether existing schema should be dropped
        format_price_files: whether price source files should be formatted
        format_commodities_files: whether commodities  files should be formatted

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

    # format commodities data is flagged: format files
    if format_commodities_files:
        format_commodities_data(
            './data/commodities/commodity_trade_statistics.csv'
        )

    # load commodities trade stats data
    load_final_commodities_tables()

    # run transformations
    load_derived_tables()

    # check tables data quality
    check_for_minimum_rows(CHECK_FOR_MINIMUM, 10, TABLES)

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
    print('\n\n📦 Loading currency exchange tables...\n')

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


def load_final_prices_tables(source: str, table: str) -> None:
    """
    Loads final stocks and ETF prices tables.

    Args:
        source: whether load from 'stocks' or 'ETFs' folder
        table:  destination table name

    Returns:
        nothing.
    """
    print(f'\n\n📦 Loading {source} prices tables...\n')

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


def load_derived_tables() -> None:
    """
    Loads derived tables data.

    Returns:
        nothing.
    """
    print(f'\n\n📦 Loading derived tables...\n')

    # render transform from currencies dates
    currencies = TRANSFORM_DATES.format(
        dim_table='dim_date',
        date_column='currency_date',
        fact_table='fact_exchange_rate'
    )

    # render transform from stock prices dates
    stocks = TRANSFORM_DATES.format(
        dim_table='dim_date',
        date_column='price_date',
        fact_table='fact_stock_price'
    )

    # render tranform from ETF prices dates
    etf = TRANSFORM_DATES.format(
        dim_table='dim_date',
        date_column='price_date',
        fact_table='fact_etf_price'
    )

    # load derived tables
    run_queries([currencies, stocks, etf])


def load_final_commodities_tables() -> None:
    """
    Loads final commodities fact and dimensions table.

    Returns:
        nothing.
    """
    print(f'\n\n📦 Loading commodities trade tables...\n')

    with open('./data/commodities/commodities-fact.csv', 'r') as input_file:
        load_data(
            input_file,
            f'currencies.fact_commodities_stats',
            columns=[
                'country_or_area',
                'year',
                'comm_code',
                'flow',
                'trade_usd',
                'weight_kg',
                'quantity'
            ]
        )

    with open('./data/commodities/commodities-dim.csv', 'r') as input_file:
        load_data(
            input_file,
            f'currencies.dim_commodity',
            columns=[
                'comm_code',
                'commodity',
                'quantity_name',
                'category'
            ]
        )

if __name__ == '__main__':

    # execute pipeline
    run(True, False, False)
