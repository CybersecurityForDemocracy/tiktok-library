# tiktok_research_api_helper

[![PyPI - Version](https://img.shields.io/pypi/v/tiktok_research_api_helper.svg)](https://pypi.org/project/tiktok_research_api_helper)

This package provides both a CLI application and python library for querying
video information from the TikTok Research API.

**This library requires TikTok Research API access. It does not provide any access by itself.**

## Requirements

Python3.11+ is **required**. Some newer features are directly used and earlier versions won't work (e.g. Walrus, type hinting chaining "|", etc., StrEnum)

# Python code usage

### Create secrets.yaml
You need to put your API credentials in yaml file which the client code will use for authentication.
Expected fields (no quotes):
```yaml
client_id: 123
client_secret: abc
client_key: abc
```

## Using the interface:
#### Construct an API query

A query is a combination of a "type (and, or, not)" with multiple Conditions ("Cond")

Each condition is a combination of a "field" (Fields, F), "value" and a operation ("Operations", "Op").
```python
from tiktok_research_api_helper.query import VideoQuery, Cond, Fields, Op

query = VideoQuery(
        and_=[
            Cond(Fields.hashtag_name, "garfield", Op.EQ),
            Cond(Fields.region_code, "US", Op.EQ),

            # Alternative version with multiple countries - Then the operation changes to "IN" instead of "EQ" (equals) as it's a list
            # the library handles list vs str natively
            # Cond(Fields.region_code, ["US", "UK"], Op.IN),
        ],
    )
```

#### TikTokApiClient provides a high-level interface to fetch all api results, and optionally store them in a database
```python
from pathlib import Path
from datetime import datetime
from tiktok_research_api_helper.query import VideoQuery, Cond, Fields, Op
from tiktok_research_api_helper.api_client import ApiClientConfig, TikTokApiClient, VideoQueryConfig

client_config = ApiClientConfig(engine=None, # No database engine configured, so
                                             # client cannot store results
                                api_credentials_file=Path("./secrets.yaml"))
api_client = TikTokApiClient.from_config(client_config)

# Now setup our query with start and end dates.
query_config = VideoQueryConfig(query=query,
                                start_date=datetime.fromisoformat("2024-03-01"),
                                end_date=datetime.fromisoformat("2024-03-02"))

# api_results_iter yields each API reponse as a parsed TikTokApiClientFetchResult.
# Iteration stops when the API indicates the query results have been fully delivered. or if client_config.max_api_requests is reached.
for result in api_client.api_results_iter(query_config):
    # do something with the result
    print(result.videos)


# Alternatively fetch_all fetches all API results and returns a single TikTokApiClientFetchResult with all API results. NOTE: this blocks until all results are fetched which could be multiple days if query results exceed daily quota limit.
api_client.fetch_all(query_config)


# If you provide a SqlAlchemy engine in the ApiClientConfig you can use TikTokApiClient to store results as they are received
api_client.fetch_and_store_all(query_config) # or equivalent call: fetch_all(query_config, store_results_after_each_response=True)
```

You can also fetch user info and comments for videos that match the qurey:
```python
query_config = VideoQueryConfig(query=query,
                                start_date=datetime.fromisoformat("2024-03-01"),
                                end_date=datetime.fromisoformat("2024-03-02"),
                                fetch_comments=True,
                                fetch_user_info=True)

# Reusing same client before.
results = api_client.fetch_all(query_config)
print('Videos: ", results.videos)
print('User info: ", results.user_info)
print('Comments: ", results.comments)
```

#### TikTokApiRequestClient and TikTokRequest provide a lower-level interface to API

##### Fetching Videos
```python
from pathlib import Path
from tiktok_research_api_helper.api_client import TikTokApiRequestClient, TikTokVideoRequest

# reads from secrets.yaml in the same directory
request_client = TikTokApiRequestClient.from_credentials_file(Path("./secrets.yaml"))
from tiktok_research_api_helper.query import VideoQuery, Cond, Fields, Op

query = VideoQuery(or_=Cond(Fields.video_id, ["7345557461438385450", "123456"], Op.IN))

# sample query
video_req = TikTokVideoRequest(
    query=query,
    start_date="20240301",
    end_date="20240329",
)

# then fetch the first page of results for the query. NOTE: this does not automatically fetch subsequent pages.
result = request_client.fetch_videos(video_req)

# to request the next page of resuls, you must create a new request with the cursor and search_id values from previous result. NOTE: make sure to check results.data['has_more'] == true
new_video_req = TikTokVideoRequest(query=query,
                cursor=result.data['cursor'],
                search_id=result.data['search_id'],
            )
result = request_client.fetch_videos(new_video_req)
```

##### Fetching Comments
```python
from tiktok_research_api_helper.api_client import TikTokCommentsRequest

video_id = "7345557461438385450"
comments_req = TikTokCommentsRequest(video_id=video_id)

result = request_client.fetch_comments(comments_req)
for comment in result.coments:
  print(comment)
```

##### Fetching user info
```python
from tiktok_research_api_helper.api_client import TikTokUserInfoRequest

username = "example"
user_info_req = TikTokUserInfoRequest(username=username)

result = request_client.fetch_user_info(user_info_req)
print(username, " user info: ", result.user_info[username])
```


# Basic CLI usage

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
account with `--region` (this flag can be provided multiple times to include
multiple regions). See tiktok API documentation for more info about this field
https://developers.tiktok.com/doc/research-api-specs-query-videos/

### Video ID
If you know the ID(s) of the video(s) you want, you can query for it directly
with `--video-id`. This can be used multiple times to query for multiple video
IDs. NOTE: you will still have to provide start and end dates (due to TikTok
Research API design).

### fetching user info
`--fetch-user-info` For each video the API returns user info is fetched for the video's creator.  (for more about what TikTok research API provides and how it is structured see https://developers.tiktok.com/doc/research-api-specs-query-user-info)

 ### Fetching comments
`--fetch-comments` For each video the API returns comments (up to the first 1000
due to API limitations) are fetched. (for more about what the API provides for comments and how the responses are structured see https://developers.tiktok.com/doc/research-api-specs-query-video-comments)
**NOTE: fetching comments can significantly increase API quota consumption beacuse potentially every video will used 10 extra API requests.**

### Limiting number of API requests (to preserve precious API quota)
by default tool has no limit on API requests. When the API indicates quota has
been exceed the tool sleeps and retries until quota resets at UTC midnight. If
you wish to limit the number of requests, say to preserve precious little API
quota, you can use the `--max-api-requests` flag which take a positive int. Once
that many requests have been made crawling will stop even if the API indicates
more results are available.

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
JSON file that will be used as the query for requests to the API, and can be
used multiple times in the same command to run those queries serially. NOTE: the
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

- Currently only video data ("Query Videos"), user info, and video comments is supported directly.

## Internals

- Long running queries are automatically split into smaller 7 days chunks. This is to avoid the 30 day limit on the TikTok API.
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
    - `fetch_comments` and `fetch_user_info` cache responses (using video ID,
      and username respectively) to reduce API requests at the cost of
      additional memory usage. This done via rudimentary dict storage with the
      video ID or username mapping to the response.

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
- Support for other data types (e.g. "User reposted videos", "User followers",
  etc)

## Why ...
- Having a query as python code inside a file?
    - To make facilitate not having to write extensive json queries in the CLI.
- Not [tiktok-research-client](https://github.com/AndersGiovanni/tiktok-research-client/tree/main)?
    - At the time of creation, the library was not available.

# Development

## Installation

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

NOTE: formatting fixes will not be applied if linter finds errors, if this
happens you can run the formatter only with `hatch fmt --formatter` (good for
when there is a linter issue the formatter can fix).

## Running jupyter notebook via hatch
`hatch run jupyter:notebook`

## Docker image
*ghcr.io/cybersecurityfordemocracy/tiktok_research_api_helper* is a very minimal
wrapper to run `tiktok-lib` in a docker container. Currently the image is built
by installing the specified version of this package from pypi (for simplicity
and transparency).

To build the image you can use the following command:
```shell
bash -c 'VERSION=<PIP VERSION SPECIFIER HERE>; docker build -t ghcr.io/cybersecurityfordemocracy/tiktok_research_api_helper:${VERSION} --build-arg VERSION=${VERSION} .'
```
This will make sure that the image is tagged with the same version specifier as
is used to install from pypi.

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

# Publishing a new version
When we are ready to publish a new version of this pacakge we do the following:
1. Update the version spec in `pyproject.toml` (kudos for make doing this process with an alpha, or release candidate first)
2. Create a release via github using the version with a preceeding `v` as the tag name (ie `v0.0.3-rc4`). If this is an alpha, release candidate, etc make sure to check "pre-release" when creating the release (see https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository)
3. Open a shell in the root of this repo for the remaing steps.
4. run `git checkout <version tag>` (this will put you in a detached HEAD state at the commit where you tagged the version in the previous step)
5. build the wheel and sdist packages: `hatch build --clean`
6. make sure the wheel and sdist files have the intended version. for example, for version `v0.0.3-rc4` the wheel file will be `dist/tiktok_research_api_helper-0.0.3rc4-py3-none-any.whl`
7. publish the packages: `hatch publish` (see https://hatch.pypa.io/1.12/publish/ for more info on how to configure)
8. build new version of docker image (NOTE: omit the leading "v" as pip does not
   accept that in a version specifier):
    ```shell
    bash -c 'VERSION=<PIP VERSION SPECIFIER HERE>; docker build -t ghcr.io/cybersecurityfordemocracy/tiktok_research_api_helper:${VERSION} --build-arg VERSION=${VERSION} .'
    ```
9. push docker image to container registery
    ```shell
    docker push ghcr.io/cybersecurityfordemocracy/tiktok_research_api_helper:${VERSION}
    ```
