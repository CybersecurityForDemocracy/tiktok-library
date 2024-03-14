from datetime import datetime

import typer
from typing_extensions import Annotated

TikTokDateFormat = Annotated[
    datetime,
    typer.Argument(formats=["%Y%m%d"]),
]
