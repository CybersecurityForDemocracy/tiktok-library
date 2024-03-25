from datetime import datetime
from pathlib import Path

import typer
from typing_extensions import Annotated

TikTokStartDateFormat = Annotated[
    str, typer.Argument(help="Start date in the format %Y%m%d (e.g. 20210101)")
]

TikTokEndDateFormat = Annotated[
    str, typer.Argument(help="End date in the format %Y%m%d (e.g. 20210101) NOT INCLUSIVE (ie start date 20210101 and end date 20210102 will only include API results from 20210101.")
]

DBFileType = Annotated[
    Path,
    typer.Argument(
        exists=False,
        file_okay=True,
        dir_okay=False,
        help="Path to the SQLite database file",
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
