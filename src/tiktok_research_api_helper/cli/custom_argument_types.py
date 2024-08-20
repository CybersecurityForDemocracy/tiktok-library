from pathlib import Path
from typing import Annotated, Optional

import typer

from tiktok_research_api_helper.api_client import ApiRateLimitWaitStrategy
from tiktok_research_api_helper.region_codes import SupportedRegions

TikTokStartDateFormat = Annotated[
    str, typer.Argument(help="Start date in the format %Y%m%d (e.g. 20210101)")
]

TikTokEndDateFormat = Annotated[
    str,
    typer.Argument(
        help=(
            "End date in the format %Y%m%d (e.g. 20210101) NOT INCLUSIVE (ie start date 20210101 "
            "and end date 20210102 will only include API results from 20210101.)"
        )
    ),
]

DBFileType = Annotated[
    Optional[Path],
    typer.Option(
        exists=False,
        file_okay=True,
        dir_okay=False,
        help="Path to the SQLite database file",
    ),
]

DBUrlType = Annotated[Optional[str], typer.Option(help="database URL for storing API results")]

JsonQueryFileListType = Annotated[
    Optional[list[Path]],
    typer.Option(
        "--query-file-json",
        exists=True,
        file_okay=True,
        dir_okay=False,
        help=(
            "Path to file query as JSON. File contents will be parsed as JSON and used directly "
            "in query of API requests. Can be used multiple times to run multiple queries serially."
        ),
    ),
]

ApiCredentialsFileType = Annotated[
    Path,
    typer.Option(
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Path to API credentials/secrets yaml file.",
    ),
]

RawResponsesOutputDir = Annotated[
    Path,
    typer.Option(
        exists=True,
        file_okay=False,
        dir_okay=True,
        help="Path for dir in which to save raw API responses",
    ),
]

ApiRateLimitWaitStrategyType = Annotated[
    ApiRateLimitWaitStrategy,
    typer.Option(
        help=(
            "Retry wait strategy when API rate limit encountered. Wait for one hour or wait til "
            "next UTC midnight (when API rate limit quota resets). NOTE: if machine goes to sleep "
            "(ie close lid on laptop) the wait time is also paused. So if you use "
            f"{ApiRateLimitWaitStrategy.WAIT_NEXT_UTC_MIDNIGHT.value} and the machine goes to "
            "sleep retry will likely wait past upcoming midnight by however long the machine was "
            "asleep"
        ),
    ),
]

RegionCodeListType = Annotated[Optional[list[SupportedRegions]], typer.Option()]

IncludeAnyHashtagListType = Annotated[
    Optional[str],
    typer.Option(
        help=(
            "A comma separated list of hashtag names. Will query API for videos that have "
            "any of these hashtag names."
        )
    ),
]

ExcludeAnyHashtagListType = Annotated[
    Optional[str],
    typer.Option(
        help=(
            "A comma separated list of hashtag names. Will query API for videos that DO NOT "
            "have any of these hashtag names."
        )
    ),
]

IncludeAllHashtagListType = Annotated[
    Optional[str],
    typer.Option(
        help=(
            "A comma separated list of hashtag names. Will query API for videos that have "
            "all of these hashtag names."
        )
    ),
]

ExcludeAllHashtagListType = Annotated[
    Optional[str],
    typer.Option(
        help=(
            "A comma separated list of hashtag names. Will query API for videos that DO NOT "
            "have all of these hashtag names."
        )
    ),
]

IncludeAnyKeywordListType = Annotated[
    Optional[str],
    typer.Option(
        help=(
            "A comma separated list of keywords. Will query API for videos that have "
            "any of these keywords."
        )
    ),
]

ExcludeAnyKeywordListType = Annotated[
    Optional[str],
    typer.Option(
        help=(
            "A comma separated list of keywords. Will query API for videos that DO NOT "
            "have any of these keywords."
        )
    ),
]

IncludeAllKeywordListType = Annotated[
    Optional[str],
    typer.Option(
        help=(
            "A comma separated list of keywords. Will query API for videos that have "
            "all of these keywords."
        )
    ),
]

ExcludeAllKeywordListType = Annotated[
    Optional[str],
    typer.Option(
        help=(
            "A comma separated list of keywords. Will query API for videos that DO NOT "
            "have all of these keywords."
        )
    ),
]

OnlyUsernamesListType = Annotated[
    Optional[str],
    typer.Option(
        help=(
            "A comma separated list of usernames. Will query API for videos that have "
            "any of these usernames."
        )
    ),
]

ExcludeUsernamesListType = Annotated[
    Optional[str],
    typer.Option(
        help=(
            "A comma separated list of usernames. Will query API for videos that DO NOT "
            "have any of these usernames."
        )
    ),
]

VideoIdListType = Annotated[
    Optional[list[int]],
    typer.Option(
        "--video-id",
        help=(
            "ID of specific video to query for. Can be used multiple times to query for multiple "
            "videos."
        ),
    ),
]

CrawlTagType = Annotated[
    Optional[str],
    typer.Option(
        help=(
            "Extra metadata for tagging the crawl of the data with a name (e.g. "
            "`Experiment_1_test_acquisition`)"
        )
    ),
]

StopAfterOneRequestFlag = Annotated[
    bool,
    typer.Option(
        help=(
            "Stop after the first request - Useful for testing. "
            "DEPRECATED please use --max-api-request=1"
        )
    ),
]

FetchUserInfoFlag = Annotated[
    bool, typer.Option(help="Fetch user info of video owners/creators returned from query.")
]

FetchCommentsFlag = Annotated[
    bool,
    typer.Option(
        help=(
            "Fetch comments for videos returned from query.  WARNING: This can greatly increase "
            "API quota consumption!  NOTE: research API will only provide the top 1000 comments."
        ),
    ),
]

MaxApiRequests = Annotated[
    Optional[int],
    typer.Option(
        help=(
            "maximum number of requests to send to the API. If this limit is reached crawl/fetch "
            "stops even if API indicates more results are present, or other types (comments, "
            "user_info) have not been fetched"
        )
    ),
]

EnableDebugLoggingFlag = Annotated[bool, typer.Option(help="Enable debug level logging")]

CatchupFromStartDate = Annotated[
    Optional[str],
    typer.Option(
        help=(
            "Date from which to attempt to catch up from for run-repeated. IE start crawling at "
            "this date with the provided time window, and then crawl without delay (aside from "
            "waiting for API quota reset) run_repeated would operate as normal (ie crawl_lag days "
            "away from current date) date in the format %Y%m%d (e.g. 20210101)"
        )
    ),
]

MaxDaysPerQueryType = Annotated[
    int,
    typer.Option(
        help=(
            "Threshold for number of days between start and end dates at which a single query will "
            "be split into multiple queries. Often the API gets overloaded and returns 500 (which "
            "still consumes request quota) if the query returns lots of videos and the date range "
            "is large. So reducing this can reduce 500 responses (and request quota consumption "
            "from those) for queries that match lots of videos. IE if this is set to 3 and the "
            "start and end date are 7 days apart the query will be split in 3 queries with start "
            "and end dates: (start, start + 3), (start + 3, start + 6), (start + 6, start + 7)"
        )
    ),
]
