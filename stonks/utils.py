"""
Data ETL pipeline utilities.
"""

import glob
import pandas as pd
import tqdm


def format_prices_source(path: str) -> None:
    """
    Formats source file of stock or ETF prices.

    Args:
        path: path to prices source file

    Returns:
        nothing.
    """
    # open data as pandas dataframe
    try:
        prices = pd.read_csv(path, index_col=None)

    # data for stock symbol is empty: ignore
    except pd.errors.EmptyDataError:
        return

    # remove useless columns
    prices.drop(['OpenInt'], axis=1, inplace=True)

    # add stock symbol as column
    prices['Stock Symbol'] = path.split('/')[-1].split('.')[0]

    # unload data
    prices.to_csv(
        path,
        columns=[
            'Stock Symbol',
            'Date',
            'Open',
            'High',
            'Low',
            'Close',
            'Volume',
        ],
        index=False,
        sep=',',
        na_rep='',
        header=False
    )


def format_prices_data(path: str) -> None:
    """
    Formats source files of stocks and ETF data.

    Args:
        path: path to data container folder

    Returns:
        nothing.
    """
    print(f'\n📦 Formatting {path.split("/")[-1]} source files...\n')

    # load stocks prices table
    stock_files = glob.glob(f'{path}/*.txt')

    # format source files on path
    for stock_file in tqdm.tqdm(stock_files):
        format_prices_source(stock_file)


if __name__ == '__main__':
    format_prices_data('./data/stocks')
    format_prices_data('./data/ETFs')
