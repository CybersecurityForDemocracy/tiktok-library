import logging
from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Optional
import json

import numpy as np
import typer
import yaml
from sqlalchemy import Engine
from tqdm.auto import tqdm
from typing_extensions import Annotated

from . import utils
from .custom_types import (
    DBFileType,
    TikTokStartDateFormat,
    TikTokEndDateFormat,
    RawResponsesOutputDir,
    QueryFileType,
    ApiCredentialsFileType,
)
from .sql import Crawl, Video, get_engine_and_create_tables
from .query import AcquitionConfig, Cond, Fields, Op, Query, TiktokRequest, TikTokApiRequestClient

APP = typer.Typer(rich_markup_mode="markdown")

_DAYS_PER_ITER = 28
_COUNT_PREVIOUS_ITERATION_REPS = -1
_DEFAULT_QUERY_FILE_PATH = Path("./query.yaml")
_DEFAULT_CREDENTIALS_FILE_PATH = Path("./secrets.yaml")


def insert_videos_from_response(
    videos: list,
    engine: Engine,
    source: Optional[list] = None,
) -> None:
    try:
        Video.custom_sqlite_upsert(videos, source=source, engine=engine)
    except Exception as e:
        logging.log(logging.ERROR, f"Error with upsert! Videos: {videos}\n Error: {e}")
        logging.log(logging.ERROR, f"Skipping Upsert")


def run_long_query(config: AcquitionConfig):
    """Runs a "long" query, defined as one that may need multiple requests to get all the data.

    Unless you have a good reason to believe otherwise, queries should default to be considered "long".
    """
    request = TiktokRequest.from_config(config, max_count=100)
    logging.warning('request.request_dict: %s', request.request_dict())
    logging.warning('request.request_json: %s', request.request_json())
    logging.warning('str(request): %s', str(request))
    request_client = TikTokApiRequestClient.from_credentials_file(credentials_file=config.api_credentials_file,
                                            raw_responses_output_dir=config.raw_responses_output_dir)
    res = request_client.fetch(request)

    if not res.videos:
        logging.log(
            logging.INFO, f"No videos in response - {res}. Query: {config.query}"
        )
        return

    crawl = Crawl.from_request(res.request_data, config.query, source=config.source)
    crawl.upload_self_to_db(config.engine)

    insert_videos_from_response(res.videos, engine=config.engine, source=config.source)

    # manual tqdm maintance
    count = 1
    global _COUNT_PREVIOUS_ITERATION_REPS
    disable_tqdm = _COUNT_PREVIOUS_ITERATION_REPS <= 0
    pbar = tqdm(total=_COUNT_PREVIOUS_ITERATION_REPS, disable=disable_tqdm)

    while crawl.has_more:
        request = TiktokRequest.from_config(
            config=config,
            max_count=100,
            cursor=crawl.cursor,
            search_id=crawl.search_id,
        )
        res = request_client.fetch(request)


        crawl.update_crawl(next_res_data=res.request_data, videos=res.videos, engine=config.engine)
        insert_videos_from_response(res.videos, source=config.source, engine=config.engine)

        pbar.update(1)
        count += 1

        if not res.videos and crawl.has_more:
            logging.log(
                logging.ERROR,
                f"No videos in response but there's still data to Crawl - Query: {config.query} \n res.request_data: {res.request_data}",
            )
        if config.stop_after_one_request:
            logging.log(logging.WARN, "Stopping after one request")
            break

    logging.info("Crawl completed.")

    pbar.close()
    _COUNT_PREVIOUS_ITERATION_REPS = count


def driver_single_day(config: AcquitionConfig):
    """Simpler driver for a single day of query"""
    assert (
        config.start_date == config.final_date
    ), "Start and final date must be the same for single day driver"

    run_long_query(config)


def main_driver(config: AcquitionConfig):
    days_per_iter = utils.int_to_days(_DAYS_PER_ITER)

    total = np.ceil((config.final_date - config.start_date).days / _DAYS_PER_ITER)

    start_date = copy(config.start_date)

    with tqdm(total=total) as pbar:

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
                source=config.source,
                raw_responses_output_dir=config.raw_responses_output_dir,
                api_credentials_file=config.api_credentials_file,
            )
            run_long_query(new_config)

            start_date += days_per_iter

            pbar.update(1)

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

    engine = get_engine_and_create_tables(db_file)

    config = AcquitionConfig(
        query=test_query,
        start_date=start_date_datetime,
        final_date=end_date_datetime,
        engine=engine,
        stop_after_one_request=True,
        source=["Testing"],
        raw_responses_output_dir=None,
        api_credentials_file=api_credentials_file,
    )
    logging.log(logging.INFO, f"Config: {config}")

    _COUNT_PREVIOUS_ITERATION_REPS = -1
    driver_single_day(config)


def get_query(query_file: Path):
    with query_file.open("r") as f:
        yaml_file = yaml.load(f, Loader=yaml.FullLoader)
    _temp = {}
    exec(yaml_file["query"], globals(), _temp)
    query = _temp["return_query"]()
    return query


@APP.command()
def run(
    # Note to self: Importing "from __future__ import annotations"
    # breaks the documentation of CLI Arguments for some reason
    start_date_str: TikTokStartDateFormat,
    end_date_str: TikTokEndDateFormat,
    db_file: DBFileType,
    stop_after_one_request: Annotated[
        bool, typer.Option(help="Stop after the first request - Useful for testing")
    ] = False,
    source: Annotated[
        str,
        typer.Option(
            help="Extra metadata for logging the source of the data (e.g. `Experiment_1_test_acquisition`)"
        ),
    ] = "",
    est_nreps: Annotated[
        int,
        typer.Option(
            help="Used for estimating # acquisitions on long running queries for progress bar"
        ),
    ] = -1,
    raw_responses_output_dir: RawResponsesOutputDir = None,
    query_file: QueryFileType = Path("query.yaml"),
    api_credentials_file: ApiCredentialsFileType = _DEFAULT_CREDENTIALS_FILE_PATH,
) -> None:
    """

    This CLI reads a **YAML file called `query.yaml`** with a query string and the API key defined in **`secrets.yaml`** in the **local directory**.
    It executes it and stores the results from the TikTok API in a local SQLite database.

    If the optional est_nreps parameter is provided, it'll be used for the first iteration of a progress bar.
    If not, the progress bar will not have a end estimation.
    """
    utils.setup_logging(file_level=logging.INFO, rich_level=logging.WARN)

    logging.log(logging.INFO, f"Arguments: {locals()}")

    # Using an actual datetime object instead of a string would not allows to
    # specify the CLI help docs in the format %Y%m%d
    start_date_datetime = datetime.strptime(start_date_str, "%Y%m%d")
    end_date_datetime = datetime.strptime(end_date_str, "%Y%m%d")

    query = get_query(query_file)

    logging.log(logging.INFO, f"Query: {query}")

    engine = get_engine_and_create_tables(db_file)

    config = AcquitionConfig(
        query=query,
        start_date=start_date_datetime,
        final_date=end_date_datetime,
        engine=engine,
        stop_after_one_request=stop_after_one_request,
        source=[source],
        raw_responses_output_dir=raw_responses_output_dir,
        api_credentials_file=api_credentials_file,
    )
    logging.log(logging.INFO, f"Config: {config}")

    _COUNT_PREVIOUS_ITERATION_REPS = est_nreps

    if config.start_date == config.final_date:
        logging.log(
            logging.WARNING,
            "Start and final date are the same - running single day driver",
        )
        driver_single_day(config)
    else:
        logging.log(logging.INFO, "Running main driver")
        main_driver(config)
