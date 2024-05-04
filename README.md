# tiktok-research

## Requirements

Python3.11+ is **required**. Some newer features are directly used and earlier versions won't work (e.g. Walrus, type hinting chaining "|", etc.), `StrEnum` class
    
## Instalation and Usage

Install with pip:

```bash
git clone <this repo>
cd tiktok-library
pip install .
```
OR you can install `hatch` (see https://hatch.pypa.io/latest/install/) and run code/tests from that:
```bash
git clone <this repo>
cd tiktok-library
hatch --env test run run # run unit tests
hatch run tiktok-lib run --db-url ... # query API


> [!CAUTION]
> This library directly loads and executes the python code in the `query.yaml` file. Do not run this library with untrusted code or users.


## Basic usage

1. This library requires TikTok Research API access. It does not provide any access by itself.
2. Create a new file `secrets.yaml` in the root folder you are running code from (you can specify a different file with `--api-credentials-file`). View the `sample_secrets.yaml` file for formatting. The client_id, client_secret and client_key are required. The library automatically manages the access token and refreshes it when needed.
3. View the `ExampleInterface.ipynb` for a quick example of interfacing with it for small queries.


## Large scale and database usage
1. For larger queries, first install [SQLite](https://www.sqlite.org/).
2. Run a test query with `tiktok-lib test` 
3. Edit the `query.yaml` file to include the query you want to run.
4. View the available commands in the run command with `tiktok-lib run --help`

You can also store files in a postgresql database. Use the `--db-url` flag to
specify the connection string.

## Limitations

- Currently only video data ("Query Videos") is supported directly. 

## Internals

- Long running queries are automatically split into smaller 28 days chunks. This is to avoid the 30 day limit on the TikTok API.
- The library automatically manages the access token and refreshes it when needed.
- TikTok research API quota is 1000 requests per day (https://developers.tiktok.com/doc/research-api-faq). When the API indicates that limit has been reached this library will retry (see `--rate-limit-wait-strategy` flag for available strategies) until quota limit resets and continue collection.
- Database
    - All "Crawls" (really each request to the API) are stored in a seperate table `crawl` and the data itself in `video`.
    - Mapping of video <-> crawl is stored in `videos_to_crawls`.
    - Hashtags are stored in a separate table (with an internal ID, NOT from the
      API) `hashtag`, and the mapping of hashtags <-> videos is stored in
      `videos_to_hashtags`
    - Effect IDs are stored simimarly to Hashtags with an associtation table
      `videos_to_effect_ids`. The naming in `effect` table can be little
      confusing because `id` is the internal database ID, and `effect_id` is the
      value from the API (which is a string, but this author has only ever seen
      ints (as strings) from API).
    - Data is written to DB after every TikTokRequest, by default containing up to 100 instances.
    - If a query tag (via the `--query-tag` flag) is provided, crawls and videos
      are associated to the query tag in `crawls_to_query_tags` and
      `videos_to_query_tags` tables respectively.

## Roadmpap
- Fix warning when retrying - Only show if the retry is unsuccessful
- Add code docs
- Add query parsing directly from CLI?
- Allow for continuing a query directly from the last run.
- Support for other data types (e.g. "Query Users")

## Why ...
- Having a query as python code inside a file?
    - To make facilitate not having to write extensive json queries in the CLI.
- Not [tiktok-research-client](https://github.com/AndersGiovanni/tiktok-research-client/tree/main)?
    - At the time of creation, the library was not available.

## Development

## Testing
To run unit tests locally (requires pytest installed):
`python3 -m pytest` OR use hatch `hatch run test:run`
To run postgresql integration test (requires docker installed, may have to run
as sudo):
`docker compose build && docker compose run postgres-integration-test && docker compose down`

## Automatic formatting with black
To check if black would change code (but not actually make changes):
`hatch run style:check`
To apply changes from black
`hatch run style:fmt`

## Alembic database schema migrations
Alembic is a tool/framework for database schema migrations. For instructions on
how to use and and create revisions see https://alembic.sqlalchemy.org/en/latest/tutorial.html

There is hatch env
for this which can be invoked like so:
```
$ hatch --env alembic run alembic ...
```
The alembic config in this repo `alembic.ini` is a basic config barely modified
from generic default. To use it you will need to define `sqlalchemy.url`. If you
want to operate on different database URLs you can use a technique documented in
https://alembic.sqlalchemy.org/en/latest/cookbook.html#run-multiple-alembic-environments-from-one-ini-file
briefly you would add something like the following to `alembic.ini`:
```
[test]
sqlalchemy.url = driver://user:pass@localhost/test_database_name

[prod]
sqlalchemy.url = driver://user:pass@localhost/prod_database_name
```

Then you can specify which database to use via the config section name:
```
$ hatch --env alembic run alembic --name test <alembic command>
```

NOTE: if a database has not had alembic run against it, but nonetheless has
up-to-date schema, alembic commands will fail saying it is not on the latest
version. You can "stamp" the database as being at HEAD (refering here to alembic
versions, NOT git commits) with the following:
```
$ hatch --env alembic run alembic --name test stamp head
```
