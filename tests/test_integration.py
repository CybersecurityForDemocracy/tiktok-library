"""Tests integration of database storage of api client retrun values (using mocked http responses)."""

import re

import attrs
import pytest
import responses
from sqlalchemy import (
    select,
)
from sqlalchemy.orm import Session

from tests.common import (
    FAKE_SECRETS_YAML_FILE,
)
from tiktok_research_api_helper import api_client
from tiktok_research_api_helper.models import (
    Video,
)


@pytest.fixture
def responses_mock():
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def mocked_access_token_responses(responses_mock):
    return responses_mock.post(
        "https://open.tiktokapis.com/v2/oauth/token/",
        json={
            "access_token": "mock_access_token_1",
            "expires_in": 7200,
            "token_type": "Bearer",
        },
    )


@pytest.fixture
def mocked_video_responses(
    responses_mock, testdata_api_videos_response_unicode_with_null_bytes_json
):
    return responses_mock.post(
        re.compile("https://open.tiktokapis.com/v2/*"),
        json=testdata_api_videos_response_unicode_with_null_bytes_json,
    )


def test_tiktok_request_client_removes_null_chars(
    basic_video_query,
    basic_video_query_config,
    basic_acquisition_config,
    testdata_api_videos_response_unicode_with_null_bytes_json,
    test_database_engine,
    mocked_access_token_responses,
    mocked_video_responses,
):
    request_client = api_client.TikTokApiRequestClient.from_credentials_file(
        FAKE_SECRETS_YAML_FILE,
        api_request_session=None,
        access_token_fetcher_session=None,
    )
    client = api_client.TikTokApiClient(
        request_client=request_client,
        config=attrs.evolve(basic_acquisition_config, engine=test_database_engine),
    )
    client.fetch_and_store_all(basic_video_query_config)
    with Session(test_database_engine) as session:
        assert session.scalars(select(Video.id).order_by(Video.id)).all() == [
            testdata_api_videos_response_unicode_with_null_bytes_json["data"]["videos"][0]["id"]
        ]
