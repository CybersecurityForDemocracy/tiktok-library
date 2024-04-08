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

> [!CAUTION]
> This library directly loads and executes the python code in the `query.yaml` file. Do not run this library with untrusted code or users.


## Basic usage

1. This library requires TikTok Research API access. It does not provide any access by itself.
2. Create a new file `secrets.yaml` in the root folder you are running code from. View the `sample_secrets.yaml` file for formatting. The client_id, client_secret and client_key are required. The library automatically manages the access token and refreshes it when needed.
3. View the `ExampleInterface.ipynb` for a quick example of interfacing with it for small queries.


## Large scale and database usage
1. For larger queries, first install [SQLite](https://www.sqlite.org/).
2. Run a test query with `tiktok-lib test` 
3. Edit the `query.yaml` file to include the query you want to run.
4. View the available commands in the run command with `tiktok-lib run --help`

## Limitations

- Currently only video data ("Query Videos") is supported directly. 

## Internals

- Long running queries are automatically split into smaller 28 days chunks. This is to avoid the 30 day limit on the TikTok API.
- The library automatically manages the access token and refreshes it when needed.
- TikTok research API quota is 1000 requests per day (https://developers.tiktok.com/doc/research-api-faq). When the API indicates that limit has been reached this library will retry (see `--rate-limit-wait-strategy` flag for available strategies) until quota limit resets and continue collection.
- Database
    - All "Crawls" are stored in a seperate table `crawl` and the data itself in `video`.
    - Data is written to DB after every TikTokRequest, by default containing up to 100 instances.

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

### Development

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
