# tiktok_research_api_helper

## Requirements

Python3.11+ is **required**. Some newer features are directly used and earlier versions won't work (e.g. Walrus, type hinting chaining "|", etc., StrEnum)
    
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
```


# Basic usage

1. This library requires TikTok Research API access. It does not provide any access by itself.
2. Create a new file `secrets.yaml` in the root folder you are running code from (you can specify a different file with `--api-credentials-file`). View the `sample_secrets.yaml` file for formatting. The client_id, client_secret and client_key are required. The library automatically manages the access token and refreshes it when needed.
3. View the `ExampleInterface.ipynb` for a quick example of interfacing with it for small queries.

## Querying
You can query the API for videos that include and/or exclude hashtags and/or
keywords with the following flags:

### hashtags
- `--include-any-hashtags`
- `--include-all-hashtags`
- `--exclude-any-hashtags`
- `--exclude-all-hashtags`

### keywords
- `--include-any-keywords`
- `--include-all-keywords`
- `--exclude-any-keywords`
- `--exclude-all-keywords`

These flags take a comma separated list of values (eg `--include-all-hashtags butter,cheese`, `--only-from-usernames amuro,roux`)

flags with `any` in the name will query the API for videos that have one or more
of the provided values. for example `--include-any-hashtags butter,cheese` would
match videos with hashtags `#butter`, `#cheese`, and/or `#butter #cheese` (ie
both). The same applies for keyword variants of these flags

flags with `all` in the name will query the API for videos that have all the
provided values, and would not match videos which only a subset of the provided
values.  for example `--include-all-hashtags butter,cheese` would match videos
with hashtags `#butter #cheese`, but would not match videos with only `#butter`
but not `#cheese` and vice versa. The same applies for keyword variants of these
flags

### usernames
You can also limit results by username. Either querying for videos only from
specific usernames or excluding videos from specific usernames. NOTE: these
flags are mutually exclusive:
- `--only-from-usernames`
- `--exclude-from-usernames`


### regions
You can also limit the videos by the region in which the use registered their
account with `--region` (this flag can be provided multiple times include
multiple regions). See tiktok API documentation for more info about this field
https://developers.tiktok.com/doc/research-api-specs-query-videos/

## Print query without sending requests to API
If you would like to preview the query that would be sent to the API (without
actually sending a request to the API) you can use the command `print-query`
like so:
```
$ tiktok-lib print-query --include-all-keywords cheese,butter --exclude-any-hashtags pasta,tomato --region US --region FR --exclude-from-usernames carb_hater,only-vegetables
```
This prints the JSON query to stdout.

## Advanced queries
If the provided querying functionality does not meet your needs or you want to
provide your own query use the `--query-file-json` flag. This takes a path to a
JSON file that will be used as the query for requests to the API. NOTE: the
provided file is NOT checked for validity.
See tiktok documentation for more info about crafting queries https://developers.tiktok.com/doc/research-api-get-started/

You can use the `print-query` command to create a starting point. For example if you
wanted to match videos about shoes with more specific search criteria you could
create a base onto which you would build with something like:
```
$ tiktok-lib print-query --include-any-keywords shoe,shoes,sneakers,pumps,heels,boots > shoes-query.json
```
then edit `shoes-query.json` as desired, and use it with
```
$ tiktok-lib run --query-json-file shoes-query.json ...
```


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
- `TikTokApiClient` provides a high-level interface for querying TikTok Research
  API
    - Handles API pagination (ie requesting results from API until API indicates
      query results have been completely delivered), access token
      fetch/refresh, and retry on request failures.
    - Client provides an iterator (`api_results_iter`) which yields each parsed
      API response, or `fetch_all` which returns all parsed results in one
      object.
    - `store_fetch_result` stores crawl and videos data to the database
    - `fetch_and_store_all` does all the above (fetching all results from API
      and storing them in database as responses are received).
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
- Allow for continuing a query directly from the last run.
- Support for other data types (e.g. "Query Users")

## Why ...
- Having a query as python code inside a file?
    - To make facilitate not having to write extensive json queries in the CLI.
- Not [tiktok-research-client](https://github.com/AndersGiovanni/tiktok-research-client/tree/main)?
    - At the time of creation, the library was not available.

# Development

## Testing
To run unit tests locally (requires pytest installed):
`python3 -m pytest` OR use hatch `hatch run test:run`

To run postgresql integration test (requires docker installed, may have to run
as sudo):
```
docker compose build && docker compose run postgres-integration-test && docker compose down
```
OR run with hatch (this runs above docker commands as sudo):
```
hatch run test:postgres-integration-test-docker-as-sudo
```

## Automatic formatting and linting with ruff
To check if ruff would change code (but not actually make changes):

`hatch fmt --check`

To apply changes from ruff:

`hatch fmt`

NOTE: formatting fixes will not be applied if linter finds errors.

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
