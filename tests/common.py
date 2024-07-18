"""NOTE: the pytest fixtures in this module should not be imported directly. they are registered as
pytest plugins in conftest.py. If this module is moved conftest.py pytest_plugins will also need to
be updated.
"""

import json

import pytest
from sqlalchemy import (
    Engine,
    select,
)

from tiktok_research_api_helper.models import (
    Base,
    Crawl,
    Hashtag,
    Video,
    get_engine_and_create_tables,
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
def testdata_api_videos_response_json():
    with open("tests/testdata/api_videos_response.json") as f:
        return json.load(f)


@pytest.fixture
def testdata_api_comments_response_json():
    with open("tests/testdata/api_comments_response.json") as f:
        return json.load(f)


def all_hashtag_names_sorted(session):
    return sorted(session.scalars(select(Hashtag.name)).all())


def all_hashtags(session):
    return session.scalars(select(Hashtag)).all()


def all_videos(session):
    return session.scalars(select(Video)).all()


def all_crawls(session):
    return session.scalars(select(Crawl)).all()
