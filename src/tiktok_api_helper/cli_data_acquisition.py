import json
import logging
from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import typer
from sqlalchemy import Engine
from typing_extensions import Annotated

from tiktok_api_helper import region_codes, utils
from tiktok_api_helper.api_client import (
    AcquitionConfig,
    ApiRateLimitWaitStrategy,
    TikTokApiRequestClient,
    TiktokRequest,
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
    Crawl,
    get_engine_and_create_tables,
    get_sqlite_engine_and_create_tables,
    upsert_videos,
)

APP = typer.Typer(rich_markup_mode="markdown")

_DAYS_PER_ITER = 28
_DEFAULT_CREDENTIALS_FILE_PATH = Path("./secrets.yaml")


def insert_videos_from_response(
    videos: Sequence[dict],
    crawl_id: int,
    engine: Engine,
    crawl_tags: Optional[list] = None,
) -> None:
    try:
        upsert_videos(videos, crawl_id=crawl_id, crawl_tags=crawl_tags, engine=engine)
    except Exception as e:
        logging.log(logging.ERROR, f"Error with upsert! Videos: {videos}\n Error: {e}")
        logging.log(logging.ERROR, "Skipping Upsert")


def run_long_query(config: AcquitionConfig):
    """Runs a "long" query, defined as one that may need multiple requests to get all the data.

    Unless you have a good reason to believe otherwise, queries should default to be considered "long".
    """
    request = TiktokRequest.from_config(config, max_count=100)
    logging.warning("request.as_json: %s", request.as_json())
    request_client = TikTokApiRequestClient.from_credentials_file(
        credentials_file=config.api_credentials_file,
        raw_responses_output_dir=config.raw_responses_output_dir,
    )
    res = request_client.fetch(request)

    if not res.videos:
        logging.log(
            logging.INFO, f"No videos in response - {res}. Query: {config.query}"
        )
        return

    crawl = Crawl.from_request(
        res.request_data, config.query, crawl_tags=config.crawl_tags
    )
    crawl.upload_self_to_db(config.engine)

    insert_videos_from_response(
        res.videos,
        crawl_id=crawl.id,
        engine=config.engine,
        crawl_tags=config.crawl_tags,
    )

    while crawl.has_more:
        request = TiktokRequest.from_config(
            config=config,
            max_count=100,
            cursor=crawl.cursor,
            search_id=crawl.search_id,
        )
        res = request_client.fetch(request)

        crawl.update_crawl(
            next_res_data=res.request_data, videos=res.videos, engine=config.engine
        )
        insert_videos_from_response(
            res.videos,
            crawl_id=crawl.id,
            crawl_tags=config.crawl_tags,
            engine=config.engine,
        )

        if not res.videos and crawl.has_more:
            logging.log(
                logging.ERROR,
                f"No videos in response but there's still data to Crawl - Query: {config.query} \n res.request_data: {res.request_data}",
            )
        if config.stop_after_one_request:
            logging.log(logging.WARN, "Stopping after one request")
            break

    logging.info("Crawl completed.")


def driver_single_day(config: AcquitionConfig):
    """Simpler driver for a single day of query"""
    assert (
        config.start_date == config.final_date
    ), "Start and final date must be the same for single day driver"

    run_long_query(config)


def main_driver(config: AcquitionConfig):
    days_per_iter = utils.int_to_days(_DAYS_PER_ITER)

    start_date = copy(config.start_date)

    while start_date < config.final_date:
        # API limit is 30, we maintain 28 to be safe
        local_end_date = start_date + days_per_iter
        local_end_date = min(local_end_date, config.final_date)

        new_config = AcquitionConfig(
            query=config.query,
            start_date=start_date,
            final_date=local_end_date,
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

    start_date_datetime = datetime.strptime("20220101", "%Y%m%d")
    end_date_datetime = datetime.strptime("20220101", "%Y%m%d")

    engine = get_sqlite_engine_and_create_tables(db_file)

    config = AcquitionConfig(
        query=test_query,
        start_date=start_date_datetime,
        final_date=end_date_datetime,
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
        raise typer.BadParameter(f"Unable to parse {query_file} as JSON: {e}")


def validate_mutually_exclusive_flags(
    flags_names_to_values: Mapping[str, Any], at_least_one_required=False
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
) -> None:
    """
    Executes it and stores the results from the TikTok API in a local SQLite database.
    """
    utils.setup_logging(file_level=logging.INFO, rich_level=logging.WARN)

    logging.log(logging.INFO, f"Arguments: {locals()}")

    # Using an actual datetime object instead of a string would not allows to
    # specify the CLI help docs in the format %Y%m%d
    start_date_datetime = datetime.strptime(start_date_str, "%Y%m%d")
    end_date_datetime = datetime.strptime(end_date_str, "%Y%m%d")

    validate_mutually_exclusive_flags(
        {"--db-url": db_url, "--db-file": db_file}, at_least_one_required=True
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
            ]
        ):
            raise typer.BadParameter(
                "--query-file-json cannot be used with any other flags that specify query "
                "conditions/parameters (such as --region, --include-any-hashtags, "
                "--include-any-keywords, etc"
            )

        query = get_query_file_json(query_file_json)
    else:
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

    config = AcquitionConfig(
        query=query,
        start_date=start_date_datetime,
        final_date=end_date_datetime,
        engine=engine,  # type: ignore - cant catch if logic above
        stop_after_one_request=stop_after_one_request,
        crawl_tags=[crawl_tag],
        raw_responses_output_dir=raw_responses_output_dir,
        api_credentials_file=api_credentials_file,
        api_rate_limit_wait_strategy=rate_limit_wait_strategy,
    )
    logging.log(logging.INFO, f"Config: {config}")

    if config.start_date == config.final_date:
        logging.log(
            logging.WARNING,
            "Start and final date are the same - running single day driver",
        )
        driver_single_day(config)
    else:
        logging.log(logging.INFO, "Running main driver")
        main_driver(config)
