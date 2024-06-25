import datetime
import logging
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler

TIKTOK_DATE_FORMAT = "%Y%m%d"


def int_to_days(x: int) -> datetime.timedelta:
    return datetime.timedelta(days=x)


def str_tiktok_date_format_to_datetime(string: str) -> datetime.datetime:
    return datetime.datetime.strptime(string, TIKTOK_DATE_FORMAT)


def date_to_tiktok_str_format(d: datetime.date | datetime.datetime) -> str:
    return d.strftime(TIKTOK_DATE_FORMAT)


def setup_logging_info_level() -> None:
    setup_logging(file_level=logging.INFO, rich_level=logging.INFO)


def setup_logging_debug_level() -> None:
    setup_logging(file_level=logging.DEBUG, rich_level=logging.DEBUG)


def setup_logging(file_level=logging.INFO, rich_level=logging.INFO) -> None:
    """Creates a new log file in ./logs/ with current date as filename, and configures logging
    format and levels."""

    file_dir = Path("./logs/")
    if not file_dir.exists():
        logging.log(logging.INFO, f"Creating log directory: {file_dir}")
        file_dir.mkdir(parents=True)

    file_name = Path(file_dir / str(datetime.datetime.now()))

    file_logger = logging.FileHandler(filename=file_name, mode="a")

    file_logger.setLevel(file_level)

    # Make sure to setup logging before importing modules that may overide the basicConfig
    logging.basicConfig(
        format="%(asctime)s,%(msecs)d %(name)s %(filename)s:%(lineno)s %(levelname)s %(message)s",
        # time.strftime (which logging uses to format asctime) does not have a directive for
        # microseconds, so we use this date format and %(asctime)s,%(msecs)d to get the microseconds
        # in the record
        datefmt="%Y-%m-%d %H:%M:%S",
        level=min(file_level, rich_level),
        handlers=[
            RichHandler(rich_tracebacks=True, level=rich_level, console=Console(stderr=True)),
            file_logger,
        ],
        force=True,
    )
