# Main Purpose of Project

We want to check the market and find our place in Exchange market.
by scarping latest_trades of each exchange and getting international data, we will find sum of volumes(IRT and USDT), count of trades, etc.
this will help us to compare each exchanges and decide which currency we should focus

# Project Layout:

[https://github.com/golang-standards/project-layout]

# Best Practices

[https://google.github.io/styleguide/go/best-practices]

# we will have a two main app:

1. scraper
2. ingester

# for scraper:

1. Kafka connection
2. function to fetch available markets
3. chunk the markets so each worker only scrap that markets
4. fetch data from the endpoint(websocket or API)

## dependency injections

1. Queue connection (we may change to another thing and its good for testing)
2. Basic Scrapper so that each of them must have some functions

# for ingester:

1. Kafka connection
2. Database Connection
3. Number of workers
4. Bach the result TO prevent heavy hit on database
5. some mechanism to prevent insertion of repeated data

- note: for binance and trending api will be little different from our default scraper

What we want to do in project?

## websockets

based on the document of each exchange, configure our scraper to check how many connection we allowed
to have and how many market we allow to pass in websocket.

## API

respect the API rate limit and its main purpose is to fill gaps if websocket connection lost

# problems

1. we need some metrics to check how stable is connections
2. place to check our logs
3. we need to close some connections, because some of the exchanges may not have any trades(closed) but fetchMarket dont say it
4. mechanism to prevent repeated data. now we are using the ReplacingMergeTree on ClickHouse
5. how we want to run it in production level. (DockerFile for each driver?)
6. we have to clean "symbol" so that we have same symbol for comparing
7. USDT/IRT rate for each trade (accuracy is not important)
8. Validation of data we are collecting or filling gap if something happened to our scraper (maybe use OHLC and live trading)
9. (we can think about it later) have a command to auto generate a basic driver to prevent duplication or complexity



# checks

1. checks we can get trade data history from api
2. mechanis of normalize and pars into driver part
3. input and output of data kafka from json to proto
4. add ohlc to our data
