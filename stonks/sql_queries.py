"""
SQL queries definitions.
"""


#
# SCHEMA MANAGEMENT
#
CREATE_SCHEMA = \
"""
CREATE SCHEMA IF NOT EXISTS currencies AUTHORIZATION dmrib;
"""

DROP_SCHEMA = \
"""
DROP SCHEMA IF EXISTS currencies CASCADE;
"""


#
# TABLES CREATION
#
CREATE_CURRENCY_FACTS_TABLE = \
"""
CREATE TABLE IF NOT EXISTS currencies.fact_exchange_rate
(
    currency_id          SERIAL PRIMARY KEY,
    currency_source      TEXT NOT NULL,
    currency_date        DATE NOT NULL,
    aud                  REAL,
    bgn                  REAL,
    brl                  REAL,
    cad                  REAL,
    chf                  REAL,
    cny                  REAL,
    czk                  REAL,
    dkk                  REAL,
    eur                  REAL,
    gbp                  REAL,
    hkd                  REAL,
    hrk                  REAL,
    huf                  REAL,
    idr                  REAL,
    ils                  REAL,
    inr                  REAL,
    isk                  REAL,
    jpy                  REAL,
    krw                  REAL,
    mxn                  REAL,
    myr                  REAL,
    nok                  REAL,
    nzd                  REAL,
    php                  REAL,
    pln                  REAL,
    ron                  REAL,
    rub                  REAL,
    sek                  REAL,
    sgd                  REAL,
    thb                  REAL,
    try                  REAL,
    usd                  REAL,
    zar                  REAL
);
"""

CREATE_DATE_DIMENSION_TABLE = \
"""
CREATE TABLE IF NOT EXISTS currencies.dim_date
(
    currency_date DATE PRIMARY KEY,
    day           INT,
    week          INT,
    month         INT,
    year          INT,
    quarter       INT,
    day_of_week   INT,
    day_of_year   INT
);
"""

CREATE_CURRENCY_DIMENSION_TABLE = \
"""
CREATE TABLE IF NOT EXISTS currencies.dim_currency
(
    currency_source TEXT NOT NULL,
    currency_name   TEXT NOT NULL,
    subunit         TEXT,
    symbol          TEXT
);
"""


CREATE_STOCKS_PRICE_FACT_TABLE = \
"""
CREATE TABLE IF NOT EXISTS currencies.fact_stock_price
(
    id           SERIAL PRIMARY KEY,
    stock_symbol TEXT NOT NULL,
    price_date   DATE NOT NULL,
    open         REAL,
    high         REAL,
    low          REAL,
    close        REAL,
    volume       BIGINT
);
"""


CREATE_ETF_PRICE_FACT_TABLE = \
"""
CREATE TABLE IF NOT EXISTS currencies.fact_etf_price
(
    id           SERIAL PRIMARY KEY,
    stock_symbol TEXT,
    price_date   DATE NOT NULL,
    open         REAL,
    high         REAL,
    low          REAL,
    close        REAL,
    volume       BIGINT
);
"""


CREATE_COMMODITIES_STATS_FACT_TABLE = \
"""
CREATE TABLE IF NOT EXISTS currencies.fact_commodities_stats
(
    id              SERIAL PRIMARY KEY,
    country_or_area TEXT,
    year            INT,
    comm_code       TEXT,
    flow            TEXT,
    trade_usd       REAL,
    weight_kg       REAL,
    quantity        REAL
);
"""


CREATE_COMMODITY_DIMENSION_TABLE = \
"""
CREATE TABLE IF NOT EXISTS currencies.dim_commodity
(
    comm_code       TEXT NOT NULL,
    commodity       TEXT,
    quantity_name   TEXT,
    category        TEXT
);
"""


#
# TRANSFORMATIONS
#
TRANSFORM_DATES = \
"""
INSERT INTO currencies.{dim_table}
SELECT DISTINCT
       {date_column},
       EXTRACT(DAY FROM {date_column}) AS day,
       EXTRACT(WEEK FROM {date_column}) AS week,
       EXTRACT(MONTH FROM {date_column}) AS month,
       EXTRACT(YEAR FROM {date_column}) AS year,
       EXTRACT(QUARTER FROM {date_column}) AS quarter,
       EXTRACT(DOW FROM {date_column}) AS day_of_week,
       EXTRACT(DOY FROM {date_column}) AS day_of_year
FROM currencies.{fact_table}
ON CONFLICT DO NOTHING;
"""


#
# DATA INTEGRITY CHECKS
#
CHECK_FOR_MINIMUM =  \
"""
SELECT *
FROM {table}
LIMIT {min};
"""


#
# PROCEDURES
#
INITIALIZE = [
    CREATE_SCHEMA,
    CREATE_CURRENCY_FACTS_TABLE,
    CREATE_DATE_DIMENSION_TABLE,
    CREATE_CURRENCY_DIMENSION_TABLE,
    CREATE_STOCKS_PRICE_FACT_TABLE,
    CREATE_ETF_PRICE_FACT_TABLE,
    CREATE_COMMODITIES_STATS_FACT_TABLE,
    CREATE_COMMODITY_DIMENSION_TABLE
]
TEARDOWN = [DROP_SCHEMA]
