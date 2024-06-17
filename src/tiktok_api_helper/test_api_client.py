from pathlib import Path
import unittest
from unittest.mock import Mock, call, MagicMock
import json
import copy
import itertools

import pytest
import requests
import pendulum
from sqlalchemy import (
    select,
)
from sqlalchemy.orm import Session

from tiktok_api_helper import api_client
from tiktok_api_helper import query
from tiktok_api_helper.sql import (
    Crawl,
    Video,
    Hashtag,
    Effect,
    CrawlTag,
    upsert_videos,
)
from tiktok_api_helper.test_utils import (
    test_database_engine,
    testdata_api_response_json,
    all_videos,
)

FAKE_SECRETS_YAML_FILE = Path("src/tiktok_api_helper/testdata/fake_secrets.yaml")


@pytest.fixture
def mock_request_session():
    mock_session = Mock(autospec=requests.Session)
    mock_session.headers = {}
    mock_session.hooks = {"response": []}
    return mock_session


@pytest.fixture
def mock_access_token_fetcher_session():
    mock_response = Mock()
    mock_response.ok = True
    mock_response.json = Mock(
        side_effect=[
            {
                "access_token": "mock_access_token_1",
                "expires_in": 7200,
                "token_type": "Bearer",
            },
            {
                "access_token": "mock_access_token_2",
                "expires_in": 7200,
                "token_type": "Bearer",
            },
        ]
    )
    mock_session = Mock(autospec=requests.Session)
    mock_session.post = Mock(return_value=mock_response)
    return mock_session


@pytest.fixture
def mock_request_session_json_decoder_error(mock_request_session):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json = Mock(side_effect=json.JSONDecodeError(msg="", doc="", pos=0))
    mock_request_session.post = Mock(return_value=mock_response)
    return mock_request_session


@pytest.fixture
def mock_request_session_rate_limit_error(mock_request_session):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json = Mock(side_effect=api_client.ApiRateLimitError)
    mock_request_session.post = Mock(return_value=mock_response)
    return mock_request_session


def test_tiktok_credentials_accepts_str_or_int_client_id():
    api_client.TiktokCredentials("client_id_1", "client_secret_1", "client_key_1")
    api_client.TiktokCredentials(123, "client_secret_1", "client_key_1")


def test_tiktok_credentials_any_value_missing_raises_value_error():
    with pytest.raises(ValueError):
        api_client.TiktokCredentials("", "", "")

    with pytest.raises(ValueError):
        api_client.TiktokCredentials("client_id_1", "client_secret_1", "")

    with pytest.raises(ValueError):
        api_client.TiktokCredentials("client_id_1", "", "client_key_1")

    with pytest.raises(ValueError):
        api_client.TiktokCredentials("", "client_secret_1", "client_key_1")

    with pytest.raises(ValueError):
        api_client.TiktokCredentials("", "", "")


def test_tiktok_api_request_client_empty_credentials_raises_value_error():
    with pytest.raises(TypeError):
        api_client.TikTokApiRequestClient(credentials=None)

    with pytest.raises(TypeError):
        api_client.TikTokApiRequestClient(credentials={})


def test_tiktok_api_request_client_from_credentials_file_factory(
    mock_request_session, mock_access_token_fetcher_session
):
    request = api_client.TikTokApiRequestClient.from_credentials_file(
        FAKE_SECRETS_YAML_FILE,
        api_request_session=mock_request_session,
        access_token_fetcher_session=mock_access_token_fetcher_session,
    )
    assert request._credentials == api_client.TiktokCredentials(
        client_id="client_id_1",
        client_secret="client_secret_1",
        client_key="client_key_1",
    )


def test_tiktok_api_request_client_attempts_token_refresh(
    mock_request_session, mock_access_token_fetcher_session
):
    assert "Authorization" not in mock_request_session.headers
    api_client.TikTokApiRequestClient.from_credentials_file(
        FAKE_SECRETS_YAML_FILE,
        api_request_session=mock_request_session,
        access_token_fetcher_session=mock_access_token_fetcher_session,
    )

    mock_access_token_fetcher_session.post.assert_called_once()
    assert mock_request_session.headers["Authorization"] == "Bearer mock_access_token_1"


@unittest.mock.patch("tenacity.nap.time.sleep")
def test_tiktok_api_request_client_retry_once_on_json_decoder_error(
    mock_sleep,
    mock_request_session_json_decoder_error,
    mock_access_token_fetcher_session,
):
    request = api_client.TikTokApiRequestClient.from_credentials_file(
        FAKE_SECRETS_YAML_FILE,
        api_request_session=mock_request_session_json_decoder_error,
        access_token_fetcher_session=mock_access_token_fetcher_session,
    )
    with pytest.raises(json.JSONDecodeError):
        request.fetch(
            api_client.TiktokRequest(query={}, start_date=None, end_date=None)
        )
    # Confirm that code retried the post request and json extraction twice (ie retried once after
    # the decode error before the exception is re-raised)
    assert mock_request_session_json_decoder_error.post.call_count == 2
    assert (
        mock_request_session_json_decoder_error.post.return_value.json.call_count == 2
    )
    mock_sleep.assert_called_once_with(0)


@unittest.mock.patch("tenacity.nap.time.sleep")
def test_tiktok_api_request_client_wait_one_hour_on_rate_limit_wait_strategy(
    mock_sleep, mock_request_session_rate_limit_error, mock_access_token_fetcher_session
):
    num_retries = 5
    request = api_client.TikTokApiRequestClient.from_credentials_file(
        FAKE_SECRETS_YAML_FILE,
        api_request_session=mock_request_session_rate_limit_error,
        access_token_fetcher_session=mock_access_token_fetcher_session,
        api_rate_limit_wait_strategy=api_client.ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS,
    )
    with pytest.raises(api_client.ApiRateLimitError):
        request.fetch(
            api_client.TiktokRequest(query={}, start_date=None, end_date=None),
            max_api_rate_limit_retries=num_retries,
        )
    # Confirm that code retried the post request and json extraction twice (ie retried once after
    # the decode error before the exception is re-raised)
    assert mock_request_session_rate_limit_error.post.call_count == num_retries
    assert (
        mock_request_session_rate_limit_error.post.return_value.json.call_count
        == num_retries
    )
    # Sleep will be called once less than num_retries because it is not called after last retry
    assert mock_sleep.call_count == num_retries - 1
    assert mock_sleep.mock_calls == [
        call(14400.0),
        call(14400.0),
        call(14400.0),
        call(14400.0),
    ]


@unittest.mock.patch("tenacity.nap.time.sleep")
def test_tiktok_api_request_client_wait_til_next_utc_midnight_on_rate_limit_wait_strategy(
    mock_sleep, mock_request_session_rate_limit_error, mock_access_token_fetcher_session
):
    # Freeze time so that we can predict time til midnight
    with pendulum.travel(freeze=True):
        expected_sleep_duration = (pendulum.tomorrow("UTC") - pendulum.now()).seconds
        num_retries = 5
        request = api_client.TikTokApiRequestClient.from_credentials_file(
            FAKE_SECRETS_YAML_FILE,
            api_request_session=mock_request_session_rate_limit_error,
            access_token_fetcher_session=mock_access_token_fetcher_session,
            api_rate_limit_wait_strategy=api_client.ApiRateLimitWaitStrategy.WAIT_NEXT_UTC_MIDNIGHT,
        )
        with pytest.raises(api_client.ApiRateLimitError):
            request.fetch(
                api_client.TiktokRequest(query={}, start_date=None, end_date=None),
                max_api_rate_limit_retries=num_retries,
            )
        # Confirm that code retried the post request and json extraction twice (ie retried once after
        # the decode error before the exception is re-raised)
        assert mock_request_session_rate_limit_error.post.call_count == num_retries
        assert (
            mock_request_session_rate_limit_error.post.return_value.json.call_count
            == num_retries
        )
        # Sleep will be called once less than num_retries because it is not called after last retry
        assert mock_sleep.call_count == num_retries - 1
        assert mock_sleep.mock_calls == [
            call(expected_sleep_duration),
            call(expected_sleep_duration),
            call(expected_sleep_duration),
            call(expected_sleep_duration),
        ]


@pytest.fixture
def mock_tiktok_responses(testdata_api_response_json):
    first_page = copy.deepcopy(testdata_api_response_json)

    second_page = copy.deepcopy(first_page)
    # Emulate API incrementing cursor by number of previous results
    second_page["data"]["cursor"] += 100
    # Modifiy video IDs so that database treats them as distinct
    for video in second_page["data"]["videos"]:
        video["id"] += 1

    last_page = copy.deepcopy(second_page)
    # Emulate API indicating this is the last page of results
    last_page["data"]["has_more"] = False
    # Emulate API incrementing cursor by number of previous results
    last_page["data"]["cursor"] += 100
    # Modifiy video IDs so that database treats them as distinct
    for video in last_page["data"]["videos"]:
        video["id"] += 1

    return [
        api_client.TikTokResponse(
            data=first_page["data"],
            videos=first_page["data"]["videos"],
            error=first_page["error"],
        ),
        api_client.TikTokResponse(
            data=second_page["data"],
            videos=second_page["data"]["videos"],
            error=second_page["error"],
        ),
        api_client.TikTokResponse(
            data=last_page["data"],
            videos=last_page["data"]["videos"],
            error=last_page["error"],
        ),
    ]


@pytest.fixture
def mock_tiktok_request_client(mock_tiktok_responses):
    mock_request_client = MagicMock(autospec=api_client.TikTokApiClient)
    mock_request_client.fetch = Mock(side_effect=mock_tiktok_responses)
    return mock_request_client


@pytest.fixture
def basic_acquisition_config():
    return api_client.AcquitionConfig(
        query=query.generate_query(include_any_hashtags="test1,test2"),
        start_date=pendulum.parse("20240601"),
        final_date=pendulum.parse("20240601"),
        engine=None,
        api_credentials_file=None,
    )


@pytest.fixture
def expected_fetch_calls(basic_acquisition_config, mock_tiktok_responses):
    return [
        call(
            api_client.TiktokRequest(
                query=basic_acquisition_config.query,
                start_date=basic_acquisition_config.start_date.strftime("%Y%m%d"),
                end_date=basic_acquisition_config.final_date.strftime("%Y%m%d"),
                max_count=basic_acquisition_config.max_count,
                is_random=False,
                cursor=None,
                search_id=None,
            )
        ),
        call(
            api_client.TiktokRequest(
                query=basic_acquisition_config.query,
                start_date=basic_acquisition_config.start_date.strftime("%Y%m%d"),
                end_date=basic_acquisition_config.final_date.strftime("%Y%m%d"),
                max_count=basic_acquisition_config.max_count,
                is_random=False,
                cursor=basic_acquisition_config.max_count,
                search_id=mock_tiktok_responses[-1].data["search_id"],
            )
        ),
        call(
            api_client.TiktokRequest(
                query=basic_acquisition_config.query,
                start_date=basic_acquisition_config.start_date.strftime("%Y%m%d"),
                end_date=basic_acquisition_config.final_date.strftime("%Y%m%d"),
                max_count=basic_acquisition_config.max_count,
                is_random=False,
                cursor=basic_acquisition_config.max_count * 2,
                search_id=mock_tiktok_responses[-1].data["search_id"],
            )
        ),
    ]


def test_tiktok_api_client_api_results_iter(
    basic_acquisition_config,
    mock_tiktok_request_client,
    mock_tiktok_responses,
    expected_fetch_calls,
):
    client = api_client.TikTokApiClient(
        request_client=mock_tiktok_request_client, config=basic_acquisition_config
    )
    for i, response in enumerate(client.api_results_iter()):
        assert response.videos == mock_tiktok_responses[i].videos
        assert response.crawl.has_more == (
            True if i < 2 else False
        ), f"hash_more: {response.crawl.has_more}, i: {i}"
        assert response.crawl.cursor == basic_acquisition_config.max_count * (i + 1)

    assert mock_tiktok_request_client.fetch.call_count == len(mock_tiktok_responses)
    assert mock_tiktok_request_client.fetch.mock_calls == expected_fetch_calls


def test_tiktok_api_client_fetch_all(
    basic_acquisition_config,
    mock_tiktok_request_client,
    mock_tiktok_responses,
    expected_fetch_calls,
):
    client = api_client.TikTokApiClient(
        request_client=mock_tiktok_request_client, config=basic_acquisition_config
    )

    response = client.fetch_all()

    assert response.videos == list(
        itertools.chain.from_iterable([r.videos for r in mock_tiktok_responses])
    )
    assert response.crawl.has_more == False
    assert response.crawl.cursor == basic_acquisition_config.max_count * len(
        mock_tiktok_responses
    )
    assert mock_tiktok_request_client.fetch.call_count == len(mock_tiktok_responses)
    assert mock_tiktok_request_client.fetch.mock_calls == expected_fetch_calls


def test_tiktok_api_client_store_fetch_result(
    test_database_engine,
    basic_acquisition_config,
    mock_tiktok_request_client,
    mock_tiktok_responses,
):
    basic_acquisition_config.engine = test_database_engine
    client = api_client.TikTokApiClient(
        request_client=mock_tiktok_request_client, config=basic_acquisition_config
    )
    fetch_result = client.fetch_and_store_all()
    # TODO(macpd): verify results in database.
    with Session(test_database_engine) as session:
        crawls = session.scalars(select(Crawl).order_by(Crawl.id)).all()
        assert len(crawls) == 1
        crawl = crawls[0]
        assert crawl.id == fetch_result.crawl.id
        assert (
            crawl.cursor
            == len(mock_tiktok_responses) * basic_acquisition_config.max_count
        )
        assert crawl.query == json.dumps(
            basic_acquisition_config.query, cls=query.QueryJSONEncoder
        )
        videos = all_videos(session)
        assert len(videos) == len(mock_tiktok_responses) * len(
            mock_tiktok_responses[0].videos
        )
        assert len(videos) == len(fetch_result.videos)
