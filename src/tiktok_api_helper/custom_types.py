from pathlib import Path

import typer
from typing_extensions import Annotated

from .api_client import ApiRateLimitWaitStrategy

TikTokStartDateFormat = Annotated[
    str, typer.Argument(help="Start date in the format %Y%m%d (e.g. 20210101)")
]

TikTokEndDateFormat = Annotated[
    str,
    typer.Argument(
        help="End date in the format %Y%m%d (e.g. 20210101) NOT INCLUSIVE (ie start date 20210101 and end date 20210102 will only include API results from 20210101.)"
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

QueryFileType = Annotated[
    Path,
    typer.Option(
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Path to yaml query file.",
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
        help=f"Retry wait strategy when API rate limit encountered. Wait for one hour or wait til next UTC midnight (when API rate limit quota resets). NOTE: if machine goes to sleep (ie close lid on laptop) the wait time is also paused. So if you use {ApiRateLimitWaitStrategy.WAIT_NEXT_UTC_MIDNIGHT.value} and the machine goes to sleep retry will likely wait past upcoming midnight by however long the machine was asleep"
    ),
]
