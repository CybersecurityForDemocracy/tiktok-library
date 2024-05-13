from pathlib import Path
from typing import Optional, List

import typer
from typing_extensions import Annotated

from .api_client import ApiRateLimitWaitStrategy
from .region_codes import SupportedRegions

TikTokStartDateFormat = Annotated[
    str, typer.Argument(help="Start date in the format %Y%m%d (e.g. 20210101)")
]

TikTokEndDateFormat = Annotated[
    str,
    typer.Argument(
        help=("End date in the format %Y%m%d (e.g. 20210101) NOT INCLUSIVE (ie start date 20210101 "
              "and end date 20210102 will only include API results from 20210101.)")
    ),
]

DBFileType = Annotated[
    Path,
    typer.Option(
        exists=False,
        file_okay=True,
        dir_okay=False,
        help="Path to the SQLite database file",
    ),
]

DBUrlType = Annotated[str, typer.Option(help="database URL for storing API results")]

JsonQueryFileType = Annotated[
    Path,
    typer.Option(
        exists=True,
        file_okay=True,
        dir_okay=False,
        help=(
            "Path to file query as JSON. File contents will be parsed as JSON and used directly "
            "in query of API requests."
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
        help=(f"Retry wait strategy when API rate limit encountered. Wait for one hour or wait til "
              f"next UTC midnight (when API rate limit quota resets). NOTE: if machine goes to "
              f"sleep (ie close lid on laptop) the wait time is also paused. So if you use "
              f"{ApiRateLimitWaitStrategy.WAIT_NEXT_UTC_MIDNIGHT.value} and the machine goes to "
              f"sleep retry will likely wait past upcoming midnight by however long the machine "
              f"was asleep")
    ),
]

RegionCodeListType = Annotated[Optional[List[SupportedRegions]], typer.Option()]

IncludeAnyHashtagListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of hashtag names. Will query API for videos that have "
            "any of these hashtag names."
        )
    ),
]

ExcludeAnyHashtagListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of hashtag names. Will query API for videos that DO NOT "
            "have any of these hashtag names."
        )
    ),
]

IncludeAllHashtagListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of hashtag names. Will query API for videos that have "
            "all of these hashtag names."
        )
    ),
]

ExcludeAllHashtagListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of hashtag names. Will query API for videos that DO NOT "
            "have all of these hashtag names."
        )
    ),
]

IncludeAnyKeywordListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of keywords. Will query API for videos that have "
            "any of these keywords."
        )
    ),
]

ExcludeAnyKeywordListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of keywords. Will query API for videos that DO NOT "
            "have any of these keywords."
        )
    ),
]

IncludeAllKeywordListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of keywords. Will query API for videos that have "
            "all of these keywords."
        )
    ),
]

ExcludeAllKeywordListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of keywords. Will query API for videos that DO NOT "
            "have all of these keywords."
        )
    ),
]

RegionCodeListType = Annotated[Optional[List[SupportedRegions]], typer.Option()]

IncludeAnyHashtagListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of hashtag names. Will query API for videos that have "
            "any of these hashtag names."
        )
    ),
]

ExcludeAnyHashtagListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of hashtag names. Will query API for videos that DO NOT "
            "have any of these hashtag names."
        )
    ),
]

IncludeAllHashtagListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of hashtag names. Will query API for videos that have "
            "all of these hashtag names."
        )
    ),
]

ExcludeAllHashtagListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of hashtag names. Will query API for videos that DO NOT "
            "have all of these hashtag names."
        )
    ),
]

IncludeAnyKeywordListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of keywords. Will query API for videos that have "
            "any of these keywords."
        )
    ),
]

ExcludeAnyKeywordListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of keywords. Will query API for videos that DO NOT "
            "have any of these keywords."
        )
    ),
]

IncludeAllKeywordListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of keywords. Will query API for videos that have "
            "all of these keywords."
        )
    ),
]

ExcludeAllKeywordListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of keywords. Will query API for videos that DO NOT "
            "have all of these keywords."
        )
    ),
]

OnlyUsernamesListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of usernames. Will query API for videos that have "
            "any of these usernames."
        )
    ),
]

ExcludeUsernamesListType = Annotated[
    str,
    typer.Option(
        help=(
            "A comma separated list of usernames. Will query API for videos that DO NOT "
            "have any of these usernames."
        )
    ),
]
