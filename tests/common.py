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


def file_contents(filename):
    with open(filename) as f:
        return f.read()


@pytest.fixture
def testdata_api_videos_response_file_contents():
    return file_contents("tests/testdata/api_videos_response.json")


@pytest.fixture
def testdata_api_videos_response_json(testdata_api_videos_response_file_contents):
    return json.loads(testdata_api_videos_response_file_contents)


@pytest.fixture
def testdata_api_videos_response_unicode_file_contents():
    return file_contents("tests/testdata/api_videos_response_unicode.json")


@pytest.fixture
def testdata_api_videos_response_unicode_json(testdata_api_videos_response_unicode_file_contents):
    return json.loads(testdata_api_videos_response_unicode_file_contents)


@pytest.fixture
def testdata_api_comments_response_file_contents():
    return file_contents("tests/testdata/api_comments_response.json")


@pytest.fixture
def testdata_api_comments_response_json(testdata_api_comments_response_file_contents):
    return json.loads(testdata_api_comments_response_file_contents)


def all_hashtag_names_sorted(session):
    return sorted(session.scalars(select(Hashtag.name)).all())


def all_hashtags(session):
    return session.scalars(select(Hashtag)).all()


def all_videos(session):
    return session.scalars(select(Video)).all()


def all_crawls(session):
    return session.scalars(select(Crawl)).all()
