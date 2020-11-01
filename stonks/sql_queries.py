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
CREATE_FACTS_TABLE = \
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
    currency_date DATE,
    day           INT,
    week          INT,
    month         INT,
    year          INT,
    quarter       INT,
    day_of_week   INT,
    day_of_year   INT
);
"""


#
# TRANSFORMATIONS
#
TRANSFORM_DATES = \
"""
INSERT INTO currencies.dim_date
SELECT DISTINCT
       currency_date,
       EXTRACT(DAY FROM currency_date) AS day,
       EXTRACT(WEEK FROM currency_date) AS week,
       EXTRACT(MONTH FROM currency_date) AS month,
       EXTRACT(YEAR FROM currency_date) AS year,
       EXTRACT(QUARTER FROM currency_date) AS quarter,
       EXTRACT(DOW FROM currency_date) AS day_of_week,
       EXTRACT(DOY FROM currency_date) AS day_of_year
FROM currencies.fact_exchange_rate;
"""


#
# PROCEDURES
#
TEARDOWN        = [DROP_SCHEMA]
INITIALIZE      = [CREATE_SCHEMA, CREATE_FACTS_TABLE, CREATE_DATE_DIMENSION_TABLE]
TRANSFORMATIONS = [TRANSFORM_DATES]
