"""NOTE: the pytest fixtures in this module should not be imported directly. they are registered as
pytest plugins in conftest.py. If this module is moved conftest.py pytest_plugins will also need to
be updated.
"""

import json
from pathlib import Path

import pendulum
import pytest
from sqlalchemy import (
    Engine,
    select,
)

from tiktok_research_api_helper import query
from tiktok_research_api_helper.api_client import ApiClientConfig, VideoQueryConfig
from tiktok_research_api_helper.models import (
    Base,
    Crawl,
    CrawlTag,
    Hashtag,
    Video,
    get_engine_and_create_tables,
)

_IN_MEMORY_SQLITE_DATABASE_URL = "sqlite://"

FAKE_SECRETS_YAML_FILE = Path("tests/testdata/fake_secrets.yaml")


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
def testdata_api_videos_response_page_1_of_2_file_contents():
    return file_contents("tests/testdata/api_videos_response_page_1_of_2.json")


@pytest.fixture
def testdata_api_videos_response_page_1_of_2_json(
    testdata_api_videos_response_page_1_of_2_file_contents,
):
    return json.loads(testdata_api_videos_response_page_1_of_2_file_contents)


@pytest.fixture
def testdata_api_videos_response_page_2_of_2_file_contents():
    return file_contents("tests/testdata/api_videos_response_page_2_of_2.json")


@pytest.fixture
def testdata_api_videos_response_page_2_of_2_json(
    testdata_api_videos_response_page_2_of_2_file_contents,
):
    return json.loads(testdata_api_videos_response_page_2_of_2_file_contents)


@pytest.fixture
def testdata_api_videos_response_unicode_with_null_bytes_file_contents():
    return file_contents("tests/testdata/api_videos_response_unicode_with_null_bytes.json")


@pytest.fixture
def testdata_api_videos_response_unicode_with_null_bytes_json(
    testdata_api_videos_response_unicode_with_null_bytes_file_contents,
):
    return json.loads(testdata_api_videos_response_unicode_with_null_bytes_file_contents)


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


@pytest.fixture
def basic_acquisition_config():
    return ApiClientConfig(
        engine=None,
        api_credentials_file=None,
    )


@pytest.fixture
def basic_video_query():
    return query.generate_query(include_any_hashtags="test1,test2")


@pytest.fixture
def basic_video_query_config(basic_video_query):
    return VideoQueryConfig(
        query=basic_video_query,
        start_date=pendulum.parse("20240601"),
        end_date=pendulum.parse("20240601"),
    )


@pytest.fixture
def mock_crawl_tags():
    return {CrawlTag(name="testing")}


@pytest.fixture
def mock_crawl(mock_crawl_tags):
    return Crawl(
        cursor=1,
        has_more=False,
        search_id="test",
        query="test",
        crawl_tags=mock_crawl_tags,
    )
