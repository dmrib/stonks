"""
Staging data loader.
"""

from datetime import datetime
from requests.exceptions import HTTPError

import csv
import json
import requests
import typing


CURRENCY_API_URL = 'https://api.exchangeratesapi.io'
CURRENCIES = sorted([
    "CAD", "HKD", "ISK", "PHP", "DKK", "HUF", "CZK", "AUD", "RON", "SEK", "IDR",
    "INR", "BRL", "RUB", "HRK", "JPY", "THB", "CHF", "SGD", "PLN", "BGN", "TRY",
    "CNY", "NOK", "NZD", "ZAR", "USD", "MXN", "ILS", "GBP", "KRW", "MYR", "EUR"
])


def fetch_yearly_exchange_rates(base: str, year: int) -> typing.Optional[str]:
    """
    Fetches the yearly exchanges rates for a given base currency.

    Args:
        base: base currency abbreviation (e.g. EUR, USD, BRL)
        year: year of currency exchanges to be fetched

    Returns:
        nothing.
    """
    # get todays date as string
    today = datetime.today().strftime('%Y-%m-%d')

    # set currency data is fetched flag
    fetched = False

    # get currency data
    while not fetched:

        # make request to exchanges rates API
        try:
            response = requests.get(
                f'{CURRENCY_API_URL}/'
                f'history?start_at={year}-01-01&end_at={today}&base={base}'
            )
            response.raise_for_status()
            fetched = True

        # error on request: log and re-raise
        except HTTPError as e:

            # no currencies exchange for given year, try next
            if e.response.status_code == 400:
                year += 1

            # unknown error: log and abort
            else:
                print(e, f'\n\nFailed fetching data for year {year} ⚠️')
                raise

    # request successful: return fetched data
    return json.loads(response.content.decode('utf-8'))


def unload_exchange_rates(destination: str, payload: dict) -> None:
    """
    Unload exchange rate data into CSV file.

    Args:
        destination: destination path to CSV file
        payload: exchange rates data payload

    Returns:
        nothing.
    """
    # get exchange rates base
    base = payload['base']

    # structure base, date and exchange rates data rows
    rows = []
    for date, rate in payload['rates'].items():
        rates = [rate.get(currency, 1.0) for currency in CURRENCIES]
        rows.append([base, date] + rates)

    # unload exchange rates data
    with open(f'{destination}currencies-{base}.csv', 'w', newline='') as out:
        writer = csv.writer(out)
        writer.writerows(rows)
