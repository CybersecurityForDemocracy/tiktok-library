import json
import logging
from collections.abc import Mapping, Sequence
from copy import copy
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Annotated, Any

import attrs
import pause
import pendulum
import typer

from tiktok_research_api_helper import region_codes, utils
from tiktok_research_api_helper.api_client import (
    DAILY_API_REQUEST_QUOTA,
    ApiClientConfig,
    ApiRateLimitWaitStrategy,
    TikTokApiClient,
    VideoQueryConfig,
)
from tiktok_research_api_helper.cli.custom_argument_types import (
    ApiCredentialsFileType,
    ApiRateLimitWaitStrategyType,
    CatchupFromStartDate,
    CrawlTagType,
    DBFileType,
    DBUrlType,
    EnableDebugLoggingFlag,
    ExcludeAllHashtagListType,
    ExcludeAllKeywordListType,
    ExcludeAnyHashtagListType,
    ExcludeAnyKeywordListType,
    ExcludeUsernamesListType,
    FetchCommentsFlag,
    FetchUserInfoFlag,
    IncludeAllHashtagListType,
    IncludeAllKeywordListType,
    IncludeAnyHashtagListType,
    IncludeAnyKeywordListType,
    JsonQueryFileType,
    MaxApiRequests,
    OnlyUsernamesListType,
    RawResponsesOutputDir,
    RegionCodeListType,
    StopAfterOneRequestFlag,
    TikTokEndDateFormat,
    TikTokStartDateFormat,
)
from tiktok_research_api_helper.models import (
    get_engine_and_create_tables,
    get_sqlite_engine_and_create_tables,
)
from tiktok_research_api_helper.query import (
    Cond,
    Fields,
    Op,
    VideoQuery,
    VideoQueryJSONEncoder,
    generate_query,
)

APP = typer.Typer(rich_markup_mode="markdown")

_DAYS_PER_ITER = 7
_DEFAULT_CREDENTIALS_FILE_PATH = Path("./secrets.yaml")


def driver_single_day(client_config: ApiClientConfig, query_config: VideoQueryConfig):
    """Simpler driver for a single day of query"""
    assert (
        query_config.start_date == query_config.end_date
    ), "Start and final date must be the same for single day driver"

    api_client = TikTokApiClient.from_config(client_config)
    api_client.fetch_and_store_all(query_config)


def main_driver(api_client_config: ApiClientConfig, query_config: VideoQueryConfig):
    days_per_iter = utils.int_to_days(_DAYS_PER_ITER)

    start_date = copy(query_config.start_date)

    api_client = TikTokApiClient.from_config(api_client_config)

    while start_date < query_config.end_date:
        # API limit is 30, we maintain 28 to be safe
        local_end_date = start_date + days_per_iter
        local_end_date = min(local_end_date, query_config.end_date)
        local_query_config = attrs.evolve(
            query_config, start_date=start_date, end_date=local_end_date
        )

        api_client.fetch_and_store_all(local_query_config)

        start_date += days_per_iter


@APP.command()
def test(
    db_file: DBFileType = Path("./test.db"),
    api_credentials_file: ApiCredentialsFileType = _DEFAULT_CREDENTIALS_FILE_PATH,
) -> None:
    """
    Test's the CLI's ability to connect to the database, create tables, acquire data and store it.
    By default, it'll create a test database "test.db" in the current directory.

    The test query is for the hashtag "snoopy" in the US.
    """
    utils.setup_logging_info_level()
    logging.log(logging.INFO, f"Arguments: {locals()}")

    test_query = VideoQuery(
        and_=[
            Cond(Fields.hashtag_name, "snoopy", Op.EQ),
            Cond(Fields.region_code, "US", Op.EQ),
        ]
    )

    logging.log(logging.INFO, f"VideoQuery: {test_query}")

    start_date_datetime = utils.str_tiktok_date_format_to_datetime("20220101")
    end_date_datetime = utils.str_tiktok_date_format_to_datetime("20220101")

    engine = get_sqlite_engine_and_create_tables(db_file)

    api_client_config = ApiClientConfig(
        engine=engine,
        max_api_requests=1,
        raw_responses_output_dir=None,
        api_credentials_file=api_credentials_file,
    )
    video_query_config = VideoQueryConfig(
        video_query=test_query,
        start_date=start_date_datetime,
        end_date=end_date_datetime,
        crawl_tags=["Testing"],
    )
    logging.info(
        "API client config: %s\nVideo query config: %s", api_client_config, video_query_config
    )

    driver_single_day(api_client_config, video_query_config)


def get_query_file_json(query_file: Path):
    with query_file.open("r") as f:
        file_contents = f.read()
    try:
        return json.loads(file_contents)
    except json.JSONDecodeError as e:
        raise typer.BadParameter(f"Unable to parse {query_file} as JSON: {e}") from None


def validate_mutually_exclusive_flags(
    flags_names_to_values: Mapping[str, Any], *, at_least_one_required: bool = False
):
    """Takes a dict of flag names -> flag values, and raises an exception if more than one or none
    specified."""

    num_values_not_none = len(list(filter(lambda x: x is not None, flags_names_to_values.values())))
    flag_names_str = ", ".join(flags_names_to_values.keys())

    if num_values_not_none > 1:
        raise typer.BadParameter(f"{flag_names_str} are mutually exclusive. Please use only one.")

    if at_least_one_required and num_values_not_none == 0:
        raise typer.BadParameter(f"Must specify one of {flag_names_str}")


def validate_region_code_flag_value(region_code_list: Sequence[str] | None):
    if region_code_list is None or not region_code_list:
        return

    for region_code in region_code_list:
        if not region_codes.is_supported(region_code):
            raise typer.BadParameter(f'provide region code "{region_code}" invalid.')


@APP.command()
def print_query(
    region: RegionCodeListType = None,
    include_any_hashtags: IncludeAnyHashtagListType | None = None,
    exclude_any_hashtags: ExcludeAnyHashtagListType | None = None,
    include_all_hashtags: IncludeAllHashtagListType | None = None,
    exclude_all_hashtags: ExcludeAllHashtagListType | None = None,
    include_any_keywords: IncludeAnyKeywordListType | None = None,
    exclude_any_keywords: ExcludeAnyKeywordListType | None = None,
    include_all_keywords: IncludeAllKeywordListType | None = None,
    exclude_all_keywords: ExcludeAllKeywordListType | None = None,
    only_from_usernames: OnlyUsernamesListType | None = None,
    exclude_from_usernames: ExcludeUsernamesListType | None = None,
) -> None:
    """Prints to stdout the query generated from flags. Useful for creating a base from which to
    build more complex custom JSON queries."""
    if not any(
        [
            include_any_hashtags,
            exclude_any_hashtags,
            include_all_hashtags,
            exclude_all_hashtags,
            include_any_keywords,
            exclude_any_keywords,
            include_all_keywords,
            exclude_all_keywords,
            only_from_usernames,
            exclude_from_usernames,
        ]
    ):
        raise typer.BadParameter(
            "must specify at least one of [--include-any-hashtags, --exclude-any-hashtags, "
            "--include-all-hashtags, --exclude-all-hashtags, --include-any-keywords, "
            "--include-all-keywords, --exclude-any-keywords, --exclude-all-keywords, "
            "--include-any-usernames, --include-all-usernames, --exclude-any-usernames, "
            "--exclude-all-usernames]"
        )
    validate_mutually_exclusive_flags(
        {
            "--include-any-hashtags": include_any_hashtags,
            "--include-all-hashtags": include_all_hashtags,
        }
    )
    validate_mutually_exclusive_flags(
        {
            "--exclude-any-hashtags": exclude_any_hashtags,
            "--exclude-all-hashtags": exclude_all_hashtags,
        }
    )
    validate_mutually_exclusive_flags(
        {
            "--include-any-keywords": include_any_keywords,
            "--include-all-keywords": include_all_keywords,
        }
    )
    validate_mutually_exclusive_flags(
        {
            "--exclude-any-keywords": exclude_any_keywords,
            "--exclude-all-keywords": exclude_all_keywords,
        }
    )
    validate_mutually_exclusive_flags(
        {
            "--only-from-usernames": only_from_usernames,
            "--exclude-from-usernames": exclude_from_usernames,
        }
    )
    validate_region_code_flag_value(region)

    query = generate_query(
        region_codes=region,
        include_any_hashtags=include_any_hashtags,
        include_all_hashtags=include_all_hashtags,
        exclude_any_hashtags=exclude_any_hashtags,
        exclude_all_hashtags=exclude_all_hashtags,
        include_any_keywords=include_any_keywords,
        include_all_keywords=include_all_keywords,
        exclude_any_keywords=exclude_any_keywords,
        exclude_all_keywords=exclude_all_keywords,
        only_from_usernames=only_from_usernames,
        exclude_from_usernames=exclude_from_usernames,
    )

    print(json.dumps(query, cls=VideoQueryJSONEncoder, indent=2))


@APP.command()
def run_repeated(
    crawl_span: Annotated[int, typer.Option(help="How many days between start and end dates")],
    crawl_lag: Annotated[
        int,
        typer.Option(
            help=(
                "Number of days behind/prior current date for start date. eg if 3 and crawl "
                "execution begins 2024-06-04, crawl would use start date 2024-06-01"
            )
        ),
    ] = 1,
    repeat_interval: Annotated[int, typer.Option(help="How many days between crawls.")] = 1,
    db_file: DBFileType | None = None,
    db_url: DBUrlType | None = None,
    crawl_tag: CrawlTagType | None = None,
    raw_responses_output_dir: RawResponsesOutputDir | None = None,
    query_file_json: JsonQueryFileType | None = None,
    api_credentials_file: ApiCredentialsFileType = _DEFAULT_CREDENTIALS_FILE_PATH,
    rate_limit_wait_strategy: ApiRateLimitWaitStrategyType = (
        ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS
    ),
    region: RegionCodeListType = None,
    include_any_hashtags: IncludeAnyHashtagListType | None = None,
    exclude_any_hashtags: ExcludeAnyHashtagListType | None = None,
    include_all_hashtags: IncludeAllHashtagListType | None = None,
    exclude_all_hashtags: ExcludeAllHashtagListType | None = None,
    include_any_keywords: IncludeAnyKeywordListType | None = None,
    exclude_any_keywords: ExcludeAnyKeywordListType | None = None,
    include_all_keywords: IncludeAllKeywordListType | None = None,
    exclude_all_keywords: ExcludeAllKeywordListType | None = None,
    only_from_usernames: OnlyUsernamesListType | None = None,
    exclude_from_usernames: ExcludeUsernamesListType | None = None,
    fetch_user_info: FetchUserInfoFlag | None = None,
    fetch_comments: FetchCommentsFlag | None = None,
    catch_up_from_start_date: CatchupFromStartDate | None = None,
    debug: EnableDebugLoggingFlag = False,
) -> None:
    """
    Repeatedly queries TikTok API and stores the results in specified database, advancing the crawl
    window (ie start and end dates) for each new crawl.
    """
    if crawl_span < 0:
        raise typer.BadParameter("Number of days for crawl span must be positive")
    if repeat_interval < 0:
        raise typer.BadParameter("Crawl interval must be positive")
    if crawl_lag < 0:
        raise typer.BadParameter("Lag must be positive")

    if debug:
        utils.setup_logging_debug_level()
    else:
        utils.setup_logging_info_level()

    # partial function of run that has all the static args which can be used in both catch up to
    # date, and repeat on interval mode.
    crawl_func = partial(
        run,
        db_file=db_file,
        db_url=db_url,
        crawl_tag=crawl_tag,
        raw_responses_output_dir=raw_responses_output_dir,
        query_file_json=query_file_json,
        api_credentials_file=api_credentials_file,
        rate_limit_wait_strategy=rate_limit_wait_strategy,
        region=region,
        include_any_hashtags=include_any_hashtags,
        exclude_any_hashtags=exclude_any_hashtags,
        include_all_hashtags=include_all_hashtags,
        exclude_all_hashtags=exclude_all_hashtags,
        include_any_keywords=include_any_keywords,
        exclude_any_keywords=exclude_any_keywords,
        include_all_keywords=include_all_keywords,
        exclude_all_keywords=exclude_all_keywords,
        only_from_usernames=only_from_usernames,
        exclude_from_usernames=exclude_from_usernames,
        fetch_user_info=fetch_user_info,
        fetch_comments=fetch_comments,
        debug=debug,
        # Do not setup logging again so that we keep the current log file.
        init_logging=False,
    )

    if catch_up_from_start_date:
        start_date = utils.str_tiktok_date_format_to_datetime(catch_up_from_start_date)
        crawl_date_window = utils.make_crawl_date_window(
            crawl_span=crawl_span, crawl_lag=crawl_lag, start_date=start_date
        )
        while utils.crawl_date_window_is_behind_today(crawl_date_window, crawl_lag):
            logging.info(
                "Still catching up from %s (with %s crawl_lag), will begin next run immediately.",
                catch_up_from_start_date,
                crawl_lag,
            )
            crawl_func(
                start_date_str=utils.date_to_tiktok_str_format(crawl_date_window.start_date),
                end_date_str=utils.date_to_tiktok_str_format(crawl_date_window.end_date),
                # When trying to catch up we do not limit number of api requests
                max_api_requests=None,
            )

            crawl_date_window = utils.make_crawl_date_window(
                crawl_span=crawl_span, crawl_lag=crawl_lag, start_date=crawl_date_window.end_date
            )
        logging.info("We have caught up to today minus crawl lag")

    # Repeat on interval
    while True:
        crawl_date_window = utils.make_crawl_date_window(crawl_span=crawl_span, crawl_lag=crawl_lag)

        execution_start_time = pendulum.now()
        crawl_func(
            start_date_str=utils.date_to_tiktok_str_format(crawl_date_window.start_date),
            end_date_str=utils.date_to_tiktok_str_format(crawl_date_window.end_date),
            # When repeating on intervals we limit requests to amount of API quota in that number of
            # days
            max_api_requests=(DAILY_API_REQUEST_QUOTA * repeat_interval),
        )

        wait_until_repeat_interval_elapsed(execution_start_time, repeat_interval)


def wait_until_repeat_interval_elapsed(
    execution_start_time: datetime, repeat_interval: int
) -> None:
    next_execution = execution_start_time.add(days=repeat_interval)
    logging.debug("next_execution: %s, %s", next_execution, next_execution.diff_for_humans())
    if pendulum.now() >= next_execution:
        logging.warning(
            "Previous crawl started at %s and took longer than repeat_interval %s. starting now",
            execution_start_time,
            repeat_interval,
        )
        return

    logging.info("Sleeping until %s", next_execution)
    pause.until(next_execution)


@APP.command()
def run(
    # Note to self: Importing "from __future__ import annotations"
    # breaks the documentation of CLI Arguments for some reason
    start_date_str: TikTokStartDateFormat,
    end_date_str: TikTokEndDateFormat,
    db_file: DBFileType | None = None,
    db_url: DBUrlType | None = None,
    stop_after_one_request: StopAfterOneRequestFlag = False,
    max_api_requests: MaxApiRequests | None = None,
    crawl_tag: CrawlTagType | None = None,
    raw_responses_output_dir: RawResponsesOutputDir | None = None,
    query_file_json: JsonQueryFileType | None = None,
    api_credentials_file: ApiCredentialsFileType = _DEFAULT_CREDENTIALS_FILE_PATH,
    rate_limit_wait_strategy: ApiRateLimitWaitStrategyType = (
        ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS
    ),
    region: RegionCodeListType | None = None,
    include_any_hashtags: IncludeAnyHashtagListType | None = None,
    exclude_any_hashtags: ExcludeAnyHashtagListType | None = None,
    include_all_hashtags: IncludeAllHashtagListType | None = None,
    exclude_all_hashtags: ExcludeAllHashtagListType | None = None,
    include_any_keywords: IncludeAnyKeywordListType | None = None,
    exclude_any_keywords: ExcludeAnyKeywordListType | None = None,
    include_all_keywords: IncludeAllKeywordListType | None = None,
    exclude_all_keywords: ExcludeAllKeywordListType | None = None,
    only_from_usernames: OnlyUsernamesListType | None = None,
    exclude_from_usernames: ExcludeUsernamesListType | None = None,
    fetch_user_info: FetchUserInfoFlag | None = None,
    fetch_comments: FetchCommentsFlag | None = None,
    debug: EnableDebugLoggingFlag = False,
    # Skips logging init/setup. Hidden because this is intended for other commands that setup
    # logging and then call this as a function.
    init_logging: Annotated[bool, typer.Option(hidden=True)] = True,
) -> None:
    """
    Queries TikTok API and stores the results in specified database.
    """
    if init_logging:
        if debug:
            utils.setup_logging_debug_level()
        else:
            utils.setup_logging_info_level()

    if stop_after_one_request:
        logging.error("--stop_after_one_request is deprecated, please use --max-api-requests=1")

    if stop_after_one_request and max_api_requests:
        raise typer.BadParameter(
            "--stop-after-one-request and --max-api-requests are mutually exclusive. Please use "
            "only one."
        )

    logging.log(logging.INFO, f"Arguments: {locals()}")

    # Using an actual datetime object instead of a string would not allows to
    # specify the CLI help docs in the format %Y%m%d
    start_date_datetime = utils.str_tiktok_date_format_to_datetime(start_date_str)
    end_date_datetime = utils.str_tiktok_date_format_to_datetime(end_date_str)

    validate_mutually_exclusive_flags(
        {"--db-url": db_url, "--db-file": db_file}, at_least_one_required=True
    )

    if query_file_json:
        if any(
            [
                include_any_hashtags,
                exclude_any_hashtags,
                include_all_hashtags,
                exclude_all_hashtags,
                include_any_keywords,
                exclude_any_keywords,
                include_all_keywords,
                exclude_all_keywords,
                only_from_usernames,
                exclude_from_usernames,
                region,
            ]
        ):
            raise typer.BadParameter(
                "--query-file-json cannot be used with any other flags that specify query "
                "conditions/parameters (such as --region, --include-any-hashtags, "
                "--include-any-keywords, etc"
            )

        query = get_query_file_json(query_file_json)
    else:
        validate_mutually_exclusive_flags(
            {
                "--include-any-hashtags": include_any_hashtags,
                "--include-all-hashtags": include_all_hashtags,
            }
        )
        validate_mutually_exclusive_flags(
            {
                "--exclude-any-hashtags": exclude_any_hashtags,
                "--exclude-all-hashtags": exclude_all_hashtags,
            }
        )
        validate_mutually_exclusive_flags(
            {
                "--include-any-keywords": include_any_keywords,
                "--include-all-keywords": include_all_keywords,
            }
        )
        validate_mutually_exclusive_flags(
            {
                "--exclude-any-keywords": exclude_any_keywords,
                "--exclude-all-keywords": exclude_all_keywords,
            }
        )
        validate_mutually_exclusive_flags(
            {
                "--only-from-usernames": only_from_usernames,
                "--exclude-from-usernames": exclude_from_usernames,
            }
        )

        validate_region_code_flag_value(region)

        query = generate_query(
            region_codes=region,
            include_any_hashtags=include_any_hashtags,
            include_all_hashtags=include_all_hashtags,
            exclude_any_hashtags=exclude_any_hashtags,
            exclude_all_hashtags=exclude_all_hashtags,
            include_any_keywords=include_any_keywords,
            include_all_keywords=include_all_keywords,
            exclude_any_keywords=exclude_any_keywords,
            exclude_all_keywords=exclude_all_keywords,
            only_from_usernames=only_from_usernames,
            exclude_from_usernames=exclude_from_usernames,
        )

    logging.log(logging.INFO, f"VideoQuery: {query}")

    if db_url:
        engine = get_engine_and_create_tables(db_url)
    elif db_file:
        engine = get_sqlite_engine_and_create_tables(db_file)

    api_client_config = ApiClientConfig(
        engine=engine,  # type: ignore - cant catch if logic above
        max_api_requests=1 if stop_after_one_request else max_api_requests,
        raw_responses_output_dir=raw_responses_output_dir,
        api_credentials_file=api_credentials_file,
        api_rate_limit_wait_strategy=rate_limit_wait_strategy,
    )
    query_config = VideoQueryConfig(
        query=query,
        start_date=start_date_datetime,
        end_date=end_date_datetime,
        crawl_tags=[crawl_tag] if crawl_tag else None,
        fetch_user_info=fetch_user_info,
        fetch_comments=fetch_comments,
    )
    logging.info("API client config: %s\nVideo query config: %s", api_client_config, query_config)

    if query_config.start_date == query_config.end_date:
        logging.info(
            "Start and final date are the same - running single day driver",
        )
        driver_single_day(api_client_config, query_config)
    else:
        logging.info("Running main driver")
        main_driver(api_client_config, query_config)
