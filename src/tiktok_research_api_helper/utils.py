import datetime
import logging
from collections import namedtuple
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler

TIKTOK_DATE_FORMAT = "%Y%m%d"
# time.strftime (which logging uses to format asctime) does not have a directive for microseconds,
# so we use this date format and %(asctime)s,%(msecs)d to get the microseconds in the record
DEFAULT_LOG_FORMAT = (
    "%(asctime)s,%(msecs)d %(name)s %(filename)s:%(lineno)s %(levelname)s %(message)s"
)
# This format is similar to above with addition of function name
DEBUG_LOG_FORMAT = (
    "%(asctime)s,%(msecs)d %(name)s %(filename)s:%(lineno)s->%(funcName)s() %(levelname)s "
    "%(message)s"
)

CrawlDateWindow = namedtuple("CrawlDateWindow", ["start_date", "end_date"])


def int_to_days(x: int) -> datetime.timedelta:
    return datetime.timedelta(days=x)


def str_tiktok_date_format_to_datetime(string: str) -> datetime.datetime:
    return datetime.datetime.strptime(string, TIKTOK_DATE_FORMAT)


def date_to_tiktok_str_format(d: datetime.date | datetime.datetime) -> str:
    return d.strftime(TIKTOK_DATE_FORMAT)


def make_crawl_date_window(
    crawl_span: int, crawl_lag: int, start_date: datetime.date = None
) -> CrawlDateWindow:
    """Returns a CrawlDateWindow with an end_date and start_date crawl_span days apart. If
    start_date is specified it is used as the new window's start date, otherwise the window's start
    will be today - (crawl_lag + crawl_span)
    """
    if crawl_span <= 0:
        raise ValueError("crawl_span must be non-negative")
    if crawl_lag <= 0:
        raise ValueError("crawl_lag must be non-negative")

    if start_date is None:
        start_date = datetime.date.today() - (
            datetime.timedelta(days=crawl_lag) + datetime.timedelta(days=crawl_span)
        )

    end_date = start_date + datetime.timedelta(days=crawl_span)
    crawl_date_window = CrawlDateWindow(start_date=start_date, end_date=end_date)
    logging.debug(
        "crawl_span: %s, crawl_lag: %s, start_date: %s; %s",
        crawl_span,
        crawl_lag,
        start_date,
        crawl_date_window,
    )
    return crawl_date_window


def crawl_date_window_is_behind_today(crawl_date_window: CrawlDateWindow, crawl_lag: int) -> bool:
    end_date = crawl_date_window.end_date.date()
    today = datetime.date.today()
    today_minus_crawl_lag = today - datetime.timedelta(days=crawl_lag)
    is_behind = end_date < today_minus_crawl_lag
    logging.debug(
        "end_date: %s, today - crawl_lag (%s): %s; is behind today: %s",
        end_date,
        crawl_lag,
        today_minus_crawl_lag,
        is_behind,
    )
    return is_behind


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

    min_level = min(file_level, rich_level)

    # Make sure to setup logging before importing modules that may overide the basicConfig
    logging.basicConfig(
        format=DEBUG_LOG_FORMAT if min_level == logging.DEBUG else DEFAULT_LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
        level=min_level,
        handlers=[
            RichHandler(
                rich_tracebacks=True,
                level=rich_level,
                console=Console(stderr=True),
                # Omit rich's time and path features as we handle those in our own log
                # format
                show_time=False,
                show_path=False,
            ),
            file_logger,
        ],
        force=True,
    )
