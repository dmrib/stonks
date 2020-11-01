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


#
# PROCEDURES
#
TEARDOWN = [DROP_SCHEMA]
INITIALIZE = [CREATE_SCHEMA, CREATE_FACTS_TABLE]
