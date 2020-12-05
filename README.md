# stonks
ETL data pipeline for financial historical data 💸

(Also my Udacity's Data Engineering Nanodegree Capstone project!)


## 1. Project Scope and Data

This projects aims to create a collection of tables in a PostgreSQL database, in a data warehouse fashion, of financial related data, from four sources:

- [Foreign Currencies Exchange Rates API](https://exchangeratesapi.io/)

This is an API that provides data of foreign exchanges rates from where we can fetch data starting from a given date on exchange rates between different currencies.

In this project, we'll load the exchange rates from all available currencies, starting from 1999.

This is ingested via an API call that requests the data, that is then unloaded into CSV files, each containing the currencty exchange rates from a given base currency to each other. `currencies-USD.csv` for example will contain currency exchange rates based on US dollars to every other currency on the dataset.

- Currencies Metadata

This is a small collection of metadata for the currencies, manually gathered from Wikipedia. All available data is on a single file named `currencies-metadata.csv`.

The source file is included on source control for convenience, since this was manually collected.

- [Huge Stock Market Dataset]([https://www.kaggle.com/borismarjanovic/price-volume-data-for-all-us-stocks-etfs](https://www.kaggle.com/borismarjanovic/price-volume-data-for-all-us-stocks-etfs))

Daily price and volume data for all US-based stocks and ETFs trading on the NYSE, NASDAQ, and NYSE MKT.

There's data for both stocks and ETF, split into different folders, on which every file name begins with the corresponding [stock symbol]([https://www.investopedia.com/terms/s/stocksymbol.asp](https://www.investopedia.com/terms/s/stocksymbol.asp)).

This dataset is not included on version control due to it's size. To reproduce this pipeline it must be downloaded from Kaggle, in the link provided above.

- [Global Commodity Trade Statistics]([https://www.kaggle.com/unitednations/global-commodity-trade-statistics](https://www.kaggle.com/unitednations/global-commodity-trade-statistics))

This dataset covers import and export volumes for 5,000 commodities across most countries on Earth over the last 30 years.

It is also not included on version control due to it's size. To reproduce this pipeline it must be downloaded from Kaggle, in the link provided above.


### Intended usage

Overall, **the intended usage for this data collection is the analysis of financial market data**, aiding the investigation of interactions between currency exchange rates, stock/ETF prices and global commodities commerce.

It will be directly queried by data scientists/analysts, using standard SQL.


## 2. Data exploration and assessment

Before implementing the data pipeline to create the "data warehouse", a data exploration and quality assessment was made, and it was noted that:

- On the foreign exchange API data, for some currencies, the API does return the exchange rate between a currency and itself (1.0x), while for other currencies it does not. To fix this we enforced the inclusion of this field on the data ingestion task.
- For the currencies metadata dataset, there are some missing values for the currency representation. This was also handled on ingestion where the data loading command fills the missing fields with NULL values.
- Some stocks prices source files were empty, this situation was covered in the data loading logic.
- Also on stocks prices data, we had to obtain the stock symbol from the source file name, since it was not available in the dataset itself.
- On the commodity's stats source files, we had to remove the `,` character contained in some columns as it broke the CSV file structure, this was done on the file formatter code.


## 3. Data Model

The data for currency exchange rate is stored in a small star schema, with two dimensions tables:


### fact_currency_exchange_rate

Column|Properties|Meta
:-----:|:-----:|:-----:
currency\_id|SERIAL,PRIMARY KEY|Currency exchange rate fact unique identifier.
*currency\_source*|TEXT,NOT NULL|Base currency for exchange rate name abbreviation.
*currency\_date*|DATE,NOT NULL|Exchange rate event date.
aud|REAL|Exchange rate to australian dollar.
bgn|REAL|Exchange rate to iev.
brl|REAL|Exchange rate to real.
cad|REAL|Exchange rate to canadian dollar.
chf|REAL|Exchange rate to swiss franc.
cny|REAL|Exchange rate for renminbi.
czk|REAL|Exchange rate to koruna.
dkk|REAL|Exchange rate to danish krone.
eur|REAL|Exchange rate to euro.
gbp|REAL|Exchange rate to british pound.
hkd|REAL|Exchange rate to hong kong dollar.
hrk|REAL|Exchange rate to to kuna.
huf|REAL|Exchange rate to forint.
idr|REAL|Exchange rate to rupiah.
ils|REAL|Exchange rate to new shekel.
inr|REAL|Exchange rate to rupee.
isk|REAL|Exchange rate to island króna.
jpy|REAL|Exchange rate to yen.
krw|REAL|Exchange rate to won.
mxn|REAL|Exchange rate to mexican peso.
myr|REAL|Exchange rate to ringgit.
nok|REAL|Exchange rate to norwegian krone.
nzd|REAL|Exchange rate to new zealand dollar.
php|REAL|Exchange rate to phillipine peso.
pln|REAL|Exchange rate to złoty.
ron|REAL|Exchange rate to ieu.
rub|REAL|Exchange rate to ruble.
sek|REAL|Exchange rate to swedish krona.
sgd|REAL|Exchange rate to singapore dollar.
thb|REAL|Exchange rate to baht.
try|REAL|Exchange rate to iira.
usd|REAL|Exchange rate to US dollar.
zar|REAL|Exchange rate to rand.


### dim_date

Column|Properties|Meta
:-----:|:-----:|:-----:
*register\_date*|DATE,PRIMARY KEY|Currency exchange date.
day|INT|Currency exchange fact day.
week|INT|Currency exchange event week.
month|INT|Currency exchange event month.
year|INT|Currency exchange event year.
quarter|INT|Currency exchange event quarter.
day\_of\_week|INT|Currency exchange event day of week.
day\_of\_year|INT|Currency exchange event year.


### dim_currency

Column|Properties|Meta
:-----:|:-----:|:-----:
*currency\_source*|TEXT,PRIMARY KEY|Currency source abbreviation.
currency\_name|TEXT|Currency name.
subunit|TEXT|Currency subunit.
symbol|TEXT|Currency symbol.


The global commodities data is also modeled as a star schema with a fact table and single dimension of its own:


### fact_commodities_stats

Column|Properties|Meta
:-----:|:-----:|:-----:
id|SERIAL,PRIMARY KEY|Commodity stats fact unique identifier.
country\_or\_area|TEXT|Commodity country or area.
year|INT|Year of stats.
*comm\_code*|BIGINT|Commodity World Customs Organizations code.
flow|TEXT|Flow of trade (i.e. export or import)
trade\_usd|BIGINT|Value of trade in USD.
weight\_kg|BIGINT|Weight of the commodity in kilograms.
quantity|BIGINT|Count of the quantity of a given item.


### dim_commodity

Column|Properties|Meta
:-----:|:-----:|:-----:
*comm\_code*|TEXT, PRIMARY KEY|Commodity World Customs Organizations code.
commodity|TEXT|The description of a particular commodity code.
quantity\_name|TEXT|A description of the quantity measurement type given the type of item (i.e. number of items).
category|TEXT|Category to identify commodity.
And the stock market dataset produces two tables, one for stocks and another for ETF.


Stock and ETF data have each a fact table of it's own, and share the date dimension table with the currency exchange rate fact table.
Thus we can classify the data model as a `fact constellation schema`.

### fact_stock_price

Column|Properties|Meta
:-----:|:-----:|:-----:
id|SERIAL,PRIMARY KEY|Stock price fact unique identifier.
stock\_symbol|TEXT,NOT NULL|Stock symbol.
*price\_date*|DATE,NOT NULL|Stock price fact date.
open|REAL|Stock open price.
high|REAL|Stock highest price for day.
low|REAL|Stock lowest price for day.
close|REAL|Stock close price.
volume|BIGINT|Stock volume.

### fact_etf_price

Column|Properties|Meta
:-----:|:-----:|:-----:
id|SERIAL,PRIMARY KEY|ETF price fact unique identifier.
stock\_symbol|TEXT,NOT NULL|Stock symbol.
*price\_date*|DATE,NOT NULL|ETF price fact date.
open|REAL|ETF open price.
high|REAL|ETF highest price for day.
low|REAL|ETF lowest price for day.
close|REAL|ETF close price.
volume|BIGINT|ETF volume.


The reasoning behind using a fact constelation schema lies on the advantages of this model for this use case, such as:


- Query performance

    Because a star schema database has a small number of tables and clear join paths, queries run faster than they do against an heavily normalized OLTP system.

    In a star schema database design, the dimensions are linked only through the central fact table. When two dimension tables are used in a query, only one join path, intersecting the fact table, exists between those two tables. This design feature enforces accurate and consistent query results.

- Load performance and administration

    Structural simplicity also reduces the time required to load large batches of data into a star schema database.

- Built-in referential integrity

    A star schema has **referential integrity built in when data is loaded**. Referential integrity is enforced because each record in a dimension table has a unique primary key, and all keys in the fact tables are legitimate foreign keys drawn from the dimension tables.

- Easily understood

    A star schema is easy to understand and navigate, with dimensions joined only through the fact table.

Note that for the current data model, there are some dimensions that does not have further values to justify a level of normalization, and so the dimension is left as it is on the fact table.


## 4. Running the ETL pipeline

### Prerequisites

- Python 3.8+
- Pipenv
- Docker and Docker-compose


### How to run

1. Make sure you have the required raw data in the `commodities` folder. Extract the stocks prices into a `data/stocks/` folder and the ETFs into `data/ETFs`. Commodities should be placed under `data/commodities/`.

2. Install dependencies by running `pipenv install` in the project root folder.

3. Set up credentials in the Docker-file and spin up the database infra by running `docker-compose up -d`.

4. Execute the ETL pipeline by running `pipenv run stonks/pipeline` from the repo base path.

OBS.: Unit tests can be run with `pipenv run python -q -m unittest tests/*.py`


## 5. Write up

- What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?

The goal is to provide a dataset for data scientists explore the relatioships between currency exchange rates, ETF/stocks prices and commodities commerce activity.

Queries would be typical of a star schema model, calculating aggregates on the fact tables and enhancing data with dimensions.

Spark could be incorporated to increase the data transformation performance, especially if the source data size increases. Airflow would be useful to automatically trigger the ingestion of up to date data on every source.

The model was chosen by it's simplicity, flexibity for queries and the performance gains of separating facts and dimesions tables.


- Clearly state the rationale for the choice of tools and technologies for the project.

Python was used for the ease of working with data on the language ecosystem.

Data formatting was done using Pandas, since the dataset can be stored in memory without worries.

PostgreSQL was used as the data warehouse technology for its ease to use, the possibility of running it containerized on Docker, and because the data size
does not request a distributed engine at this scale.


- Document the steps of the process

1. If requested, the database schema is droped to enable a fresh start.
2. Database schema and tables are created.
3. Currency exchange data is fetched from remote API and store as CSV files.
4. Currency exchange data is loaded to data warehouse.
5. If requested, stocks and ETFs source files are formatted.
6. Stocks and ETFs prices data is loaded to data warehouse.
7. If requested, commodities source data file is formatted.
8. Commodities data is loaded to data warehouse.
9. Date dimensions are extracted from currency exchange and stocks/ETFs data.
10. Data quality checks are run against every created table.


- How data should be updated and why?

The commodities data peridicity is yearly, so this table update would operate under this constraint.

Currency exchanges rates and stock/ETFs prices are given on a daily basis and thus can be updated at the same frequency, keeping the data updated until D-1.

Even without incorporating the commodities trade data, the remaining tables being updated would already be useful to the end user of the pipeline.

- Scaling

1. If the data was increased by 100x.

I would use Spark on the data transformations to be able to handle the dataset size without memory or perfomance issues, and migrate to a distributed database engine such as AWS Redshift to improve query performance.

2. If the pipelines were run on a daily basis by 7am.

Definitly incorporate Spark to make sure SLAs are held and use Airflow for scheduling.

3. If the database needed to be accessed by 100+ people.

I would move the infra to a cloud environment, so it would be easier to maintain. Also would use another database engine such as AWS Redshift to handle the parallel queries more effectively.


## 5. Extra perks

- CircleCI integration
- Environment provided using Docker-compose


## 6. Future improvements

- Improve unit test coverage
- Improve pipeline path parametrization


## License

MIT.
