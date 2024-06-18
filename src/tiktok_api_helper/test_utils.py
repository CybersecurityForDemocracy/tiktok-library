import json

from sqlalchemy import (
    Engine,
    select,
)
from sqlalchemy.orm import Session
import pytest

from tiktok_api_helper.sql import (
    get_engine_and_create_tables,
    Base,
    Hashtag,
    Video,
    Crawl,
)

_IN_MEMORY_SQLITE_DATABASE_URL = "sqlite://"


@pytest.fixture
def test_database_engine(database_url_command_line_arg) -> Engine:
    if database_url_command_line_arg:
        database_url = database_url_command_line_arg
    else:
        database_url = _IN_MEMORY_SQLITE_DATABASE_URL

    engine = get_engine_and_create_tables(database_url, echo=True)
    yield engine
    # Clear database after test runs and releases fixture
    Base.metadata.drop_all(engine)


@pytest.fixture
def testdata_api_response_json():
    with open("src/tiktok_api_helper/testdata/api_response.json", "r") as f:
        return json.load(f)


def all_hashtag_names_sorted(session):
    return sorted(session.scalars(select(Hashtag.name)).all())


def all_hashtags(session):
    return session.scalars(select(Hashtag)).all()


def all_videos(session):
    return session.scalars(select(Video)).all()


def all_crawls(session):
    return session.scalars(select(Crawl)).all()