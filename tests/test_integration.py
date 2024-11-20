"""Tests integration of database storage of api client retrun values (using mocked http responses)."""

import re

import attrs
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


@responses.activate
def test_tiktok_request_client_removes_null_chars(
    basic_video_query,
    basic_video_query_config,
    basic_acquisition_config,
    testdata_api_videos_response_unicode_json,
    test_database_engine,
):
    responses.post(
        "https://open.tiktokapis.com/v2/oauth/token/",
        json={
            "access_token": "mock_access_token_1",
            "expires_in": 7200,
            "token_type": "Bearer",
        },
    )
    responses.post(
        re.compile("https://open.tiktokapis.com/v2/*"),
        json=testdata_api_videos_response_unicode_json,
    )
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
            testdata_api_videos_response_unicode_json["data"]["videos"][0]["id"]
        ]
