import json
import logging
from copy import copy
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence
import time

import typer
from sqlalchemy import Engine
from typing_extensions import Annotated
import pendulum

from tiktok_api_helper import region_codes, utils
from tiktok_api_helper.api_client import (
    ApiClientConfig,
    ApiRateLimitWaitStrategy,
    TikTokApiClient,
)
from tiktok_api_helper.custom_types import (
    ApiCredentialsFileType,
    ApiRateLimitWaitStrategyType,
    DBFileType,
    DBUrlType,
    ExcludeAllHashtagListType,
    ExcludeAllKeywordListType,
    ExcludeAnyHashtagListType,
    ExcludeAnyKeywordListType,
    ExcludeUsernamesListType,
    IncludeAllHashtagListType,
    IncludeAllKeywordListType,
    IncludeAnyHashtagListType,
    IncludeAnyKeywordListType,
    JsonQueryFileType,
    OnlyUsernamesListType,
    RawResponsesOutputDir,
    RegionCodeListType,
    TikTokEndDateFormat,
    TikTokStartDateFormat,
)
from tiktok_api_helper.query import (
    Cond,
    Fields,
    Op,
    Query,
    QueryJSONEncoder,
    generate_query,
)
from tiktok_api_helper.sql import (
    get_engine_and_create_tables,
    get_sqlite_engine_and_create_tables,
)

APP = typer.Typer(rich_markup_mode="markdown")

_DAYS_PER_ITER = 28
_DEFAULT_CREDENTIALS_FILE_PATH = Path("./secrets.yaml")

def setup_logging(*, debug: bool):
    if debug:
        utils.setup_logging(file_level=logging.DEBUG, rich_level=logging.DEBUG)
    else:
        utils.setup_logging(file_level=logging.INFO, rich_level=logging.INFO)


def run_long_query(config: ApiClientConfig):
    """Runs a "long" query, defined as one that may need multiple requests to get all the data.

    Unless you have a good reason to believe otherwise, queries should default to be considered "long".
    """
    api_client = TikTokApiClient.from_config(config)
    api_client.fetch_and_store_all()


def driver_single_day(config: ApiClientConfig):
    """Simpler driver for a single day of query"""
    assert (
        config.start_date == config.end_date
    ), "Start and final date must be the same for single day driver"

    run_long_query(config)


def main_driver(config: ApiClientConfig):
    days_per_iter = utils.int_to_days(_DAYS_PER_ITER)

    start_date = copy(config.start_date)

    while start_date < config.end_date:
        # API limit is 30, we maintain 28 to be safe
        local_end_date = start_date + days_per_iter
        local_end_date = min(local_end_date, config.end_date)

        new_config = ApiClientConfig(
            query=config.query,
            start_date=start_date,
            end_date=local_end_date,
            engine=config.engine,
            stop_after_one_request=config.stop_after_one_request,
            crawl_tags=config.crawl_tags,
            raw_responses_output_dir=config.raw_responses_output_dir,
            api_credentials_file=config.api_credentials_file,
            api_rate_limit_wait_strategy=config.api_rate_limit_wait_strategy,
        )
        run_long_query(new_config)

        start_date += days_per_iter

        if config.stop_after_one_request:
            logging.log(logging.WARN, "Stopping after one request")
            break


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
    utils.setup_logging(file_level=logging.INFO, rich_level=logging.INFO)
    logging.log(logging.INFO, f"Arguments: {locals()}")

    test_query = Query(
        and_=[
            Cond(Fields.hashtag_name, "snoopy", Op.EQ),
            Cond(Fields.region_code, "US", Op.EQ),
        ]
    )

    logging.log(logging.INFO, f"Query: {test_query}")

    start_date_datetime = utils.str_tiktok_date_format_to_datetime("20220101")
    end_date_datetime = utils.str_tiktok_date_format_to_datetime("20220101")

    engine = get_sqlite_engine_and_create_tables(db_file)

    config = ApiClientConfig(
        query=test_query,
        start_date=start_date_datetime,
        end_date=end_date_datetime,
        engine=engine,
        stop_after_one_request=True,
        crawl_tags=["Testing"],
        raw_responses_output_dir=None,
        api_credentials_file=api_credentials_file,
    )
    logging.log(logging.INFO, f"Config: {config}")

    driver_single_day(config)


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

    num_values_not_none = len(
        list(filter(lambda x: x is not None, flags_names_to_values.values()))
    )
    flag_names_str = ", ".join(flags_names_to_values.keys())

    if num_values_not_none > 1:
        raise typer.BadParameter(
            f"{flag_names_str} are mutually exclusive. Please use only one."
        )

    if at_least_one_required and num_values_not_none == 0:
        raise typer.BadParameter(f"Must specify one of {flag_names_str}")


def validate_region_code_flag_value(region_code_list: Optional[Sequence[str]]):
    if region_code_list is None or not region_code_list:
        return

    for region_code in region_code_list:
        if not region_codes.is_supported(region_code):
            raise typer.BadParameter(f'provide region code "{region_code}" invalid.')


@APP.command()
def print_query(
    region: RegionCodeListType = None,
    include_any_hashtags: Optional[IncludeAnyHashtagListType] = None,
    exclude_any_hashtags: Optional[ExcludeAnyHashtagListType] = None,
    include_all_hashtags: Optional[IncludeAllHashtagListType] = None,
    exclude_all_hashtags: Optional[ExcludeAllHashtagListType] = None,
    include_any_keywords: Optional[IncludeAnyKeywordListType] = None,
    exclude_any_keywords: Optional[ExcludeAnyKeywordListType] = None,
    include_all_keywords: Optional[IncludeAllKeywordListType] = None,
    exclude_all_keywords: Optional[ExcludeAllKeywordListType] = None,
    only_from_usernames: Optional[OnlyUsernamesListType] = None,
    exclude_from_usernames: Optional[ExcludeUsernamesListType] = None,
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

    print(json.dumps(query, cls=QueryJSONEncoder, indent=2))

def make_crawl_span(num_days_crawl_span, num_days_lag):
    end_date = date.today() - timedelta(days=num_days_lag)
    start_date = end_date - timedelta(days=num_days_crawl_span)
    return start_date, end_date


@APP.command()
def run_scheduled(
    num_days_crawl_span: Annotated[int, typer.Option(help="How many days between start and end dates")],
    crawl_interval: Annotated[int, typer.Option(help="How many days between crawls.")], # TODO(macpd): sentinel value for start next crawl immediately
    num_days_lag: Annotated[int, typer.Option(help="Number of days behind/prior current date for start date. eg if 3 and crawl execution begins 2024-06-04, crawl would use start date 2024-06-01")] = 1,
    db_file: Optional[DBFileType] = None,
    db_url: Optional[DBUrlType] = None,
    crawl_tag: Annotated[
        str,
        typer.Option(
            help="Extra metadata for tagging the crawl of the data with a name (e.g. `Experiment_1_test_acquisition`)"
        ),
    ] = "",
    raw_responses_output_dir: Optional[RawResponsesOutputDir] = None,
    query_file_json: Optional[JsonQueryFileType] = None,
    api_credentials_file: ApiCredentialsFileType = _DEFAULT_CREDENTIALS_FILE_PATH,
    rate_limit_wait_strategy: ApiRateLimitWaitStrategyType = ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS,
    region: RegionCodeListType = None,
    include_any_hashtags: Optional[IncludeAnyHashtagListType] = None,
    exclude_any_hashtags: Optional[ExcludeAnyHashtagListType] = None,
    include_all_hashtags: Optional[IncludeAllHashtagListType] = None,
    exclude_all_hashtags: Optional[ExcludeAllHashtagListType] = None,
    include_any_keywords: Optional[IncludeAnyKeywordListType] = None,
    exclude_any_keywords: Optional[ExcludeAnyKeywordListType] = None,
    include_all_keywords: Optional[IncludeAllKeywordListType] = None,
    exclude_all_keywords: Optional[ExcludeAllKeywordListType] = None,
    only_from_usernames: Optional[OnlyUsernamesListType] = None,
    exclude_from_usernames: Optional[ExcludeUsernamesListType] = None,
    debug: Optional[bool] = False,
) -> None:
    # TODO(macpd): decide what format to use for schedule input. maybe crontiter
    if num_days_crawl_span < 0:
        raise typer.BadParameter("Number of days for crawl span must be positive")
    if crawl_interval < 0:
        raise typer.BadParameter("Crawl interval must be positive")
    if num_days_lag < 0:
        raise typer.BadParameter("Lag must be positive")

    setup_logging(debug=debug)
    while True:
        start_date, end_date = make_crawl_span(num_days_crawl_span=num_days_crawl_span, num_days_lag=num_days_lag)
        logging.info("Starting scheduled run. start_date: %s, end_date: %s", start_date, end_date)
        execution_start_time = pendulum.now()
        run(start_date_str=start_date.strftime(utils.TIKTOK_DATE_FORMAT),
            end_date_str=end_date.strftime(utils.TIKTOK_DATE_FORMAT),
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
            debug=debug,
            # Do not setup logging again so that we keep the current log file.
            init_logging=False)
        next_execution = execution_start_time + timedelta(days=crawl_interval)
        now = pendulum.now()
        seconds_to_next_execution = (next_execution - now).seconds
        logging.debug('now: %s, next_execution: %s, seconds_to_next_execution: %s', now,
                        next_execution, seconds_to_next_execution)
        if seconds_to_next_execution > 0:
            logging.info('Sleeping %s seconds', seconds_to_next_execution)
            time.sleep(seconds_to_next_execution)
        else:
            logging.warning('Previous crawl started at %s and took longer than crawl_interval %s.  starting now', execution_start_time, crawl_interval)




@APP.command()
def run(
    # Note to self: Importing "from __future__ import annotations"
    # breaks the documentation of CLI Arguments for some reason
    start_date_str: TikTokStartDateFormat,
    end_date_str: TikTokEndDateFormat,
    db_file: Optional[DBFileType] = None,
    db_url: Optional[DBUrlType] = None,
    stop_after_one_request: Annotated[
        bool, typer.Option(help="Stop after the first request - Useful for testing")
    ] = False,
    crawl_tag: Annotated[
        str,
        typer.Option(
            help="Extra metadata for tagging the crawl of the data with a name (e.g. `Experiment_1_test_acquisition`)"
        ),
    ] = "",
    raw_responses_output_dir: Optional[RawResponsesOutputDir] = None,
    query_file_json: Optional[JsonQueryFileType] = None,
    api_credentials_file: ApiCredentialsFileType = _DEFAULT_CREDENTIALS_FILE_PATH,
    rate_limit_wait_strategy: ApiRateLimitWaitStrategyType = ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS,
    region: RegionCodeListType = None,
    include_any_hashtags: Optional[IncludeAnyHashtagListType] = None,
    exclude_any_hashtags: Optional[ExcludeAnyHashtagListType] = None,
    include_all_hashtags: Optional[IncludeAllHashtagListType] = None,
    exclude_all_hashtags: Optional[ExcludeAllHashtagListType] = None,
    include_any_keywords: Optional[IncludeAnyKeywordListType] = None,
    exclude_any_keywords: Optional[ExcludeAnyKeywordListType] = None,
    include_all_keywords: Optional[IncludeAllKeywordListType] = None,
    exclude_all_keywords: Optional[ExcludeAllKeywordListType] = None,
    only_from_usernames: Optional[OnlyUsernamesListType] = None,
    exclude_from_usernames: Optional[ExcludeUsernamesListType] = None,
    debug: Optional[bool] = False,
    # TODO(macpd): hide this from --help, and maybe rename
    # Skips logging init/setup, used for other commands that setup logging and then call this as a function
    init_logging: [bool] = True,
) -> None:
    """
    Queries TikTok API and stores the results in specified database.
    """
    if init_logging:
        setup_logging(debug=debug)

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

    logging.log(logging.INFO, f"Query: {query}")

    if db_url:
        engine = get_engine_and_create_tables(db_url)
    elif db_file:
        engine = get_sqlite_engine_and_create_tables(db_file)

    config = ApiClientConfig(
        query=query,
        start_date=start_date_datetime,
        end_date=end_date_datetime,
        engine=engine,  # type: ignore - cant catch if logic above
        stop_after_one_request=stop_after_one_request,
        crawl_tags=[crawl_tag],
        raw_responses_output_dir=raw_responses_output_dir,
        api_credentials_file=api_credentials_file,
        api_rate_limit_wait_strategy=rate_limit_wait_strategy,
    )
    logging.log(logging.INFO, f"Config: {config}")

    if config.start_date == config.end_date:
        logging.log(
            logging.INFO,
            "Start and final date are the same - running single day driver",
        )
        driver_single_day(config)
    else:
        logging.log(logging.INFO, "Running main driver")
        main_driver(config)
