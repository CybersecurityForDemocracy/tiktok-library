from pathlib import Path
import unittest
from unittest.mock import Mock, call
import json

import pytest
import requests
import tenacity
import pendulum

from . import api_client

MOCK_VIDEO_DATA = [
    {
        "music_id": 6810180973628491777,
        "playlist_id": 0,
        "region_code": "US",
        "share_count": 50,
        "username": "american_ginger_redeemed",
        "hashtag_names": [
            "whatdidyouexpect",
            "viral",
            "foryou",
            "fyp",
            "prolife",
            "greenscreensticker",
            "unbornlivesmatter",
        ],
        "id": 7094381613995478318,
        "like_count": 2135,
        "view_count": 20777,
        "video_description": "Pregnancy is a natural outcome to unprotected s*x… what did you think was gonna happen? #fyp #foryou #unbornlivesmatter #viral #prolife #whatdidyouexpect #greenscreensticker",
        "comment_count": 501,
        "create_time": 1651789438,
        "effect_ids": ["0"],
    },
    {
        "video_description": "Period. #abortionismurder #fyp #prolife #LaurelRoad4Nurses #BBPlayDate #roemustgo",
        "create_time": 1651766906,
        "effect_ids": ["0"],
        "id": 7094284837128817962,
        "region_code": "US",
        "share_count": 5,
        "view_count": 5400,
        "comment_count": 72,
        "hashtag_names": [
            "fyp",
            "prolife",
            "abortionismurder",
            "LaurelRoad4Nurses",
            "BBPlayDate",
            "roemustgo",
        ],
        "like_count": 499,
        "music_id": 6865506085311088641,
        "username": "realmorganfaith",
    },
    {
        "like_count": 760,
        "music_id": 6833934234948732941,
        "username": "edenmccourt",
        "video_description": "I don’t usually talk about myself on my public pages, but I think given the current climate it is necessary. I want to help you understand that people on both sides of this debate are just normal people with normal interests and who should be treated with respect, dignity and kindness. We can disagree and still be friends. Less polarisation and more conversation. ❤️ #foryourpage #humanlikeyou",
        "view_count": 19365,
        "comment_count": 373,
        "effect_ids": ["0"],
        "id": 7094037673978973446,
        "region_code": "GB",
        "share_count": 30,
        "create_time": 1651709360,
        "hashtag_names": ["humanlikeyou", "foryourpage"],
    },
    {
        "comment_count": 402,
        "create_time": 1651614306,
        "id": 7093629419205561606,
        "like_count": 923,
        "region_code": "GB",
        "username": "edenmccourt",
        "video_description": "It do be like that tho. #fyp #roevwade #abortion",
        "view_count": 13809,
        "effect_ids": ["0"],
        "hashtag_names": ["abortion", "fyp", "roevwade"],
        "music_id": 7016913596630207238,
        "share_count": 16,
    },
]

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
    request = api_client.TikTokApiRequestClient.from_credentials_file(
        FAKE_SECRETS_YAML_FILE,
        api_request_session=mock_request_session,
        access_token_fetcher_session=mock_access_token_fetcher_session,
    )

    mock_access_token_fetcher_session.post.assert_called_once()
    assert mock_request_session.headers["Authorization"] == "Bearer mock_access_token_1"


@unittest.mock.patch('tenacity.nap.time.sleep')
def test_tiktok_api_request_client_retry_once_on_json_decoder_error(
    mock_sleep, mock_request_session_json_decoder_error, mock_access_token_fetcher_session
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

@unittest.mock.patch('tenacity.nap.time.sleep')
def test_tiktok_api_request_client_wait_one_hour_on_rate_limit_wait_strategy(
    mock_sleep, mock_request_session_rate_limit_error, mock_access_token_fetcher_session
):
    num_retries = 5
    request = api_client.TikTokApiRequestClient.from_credentials_file(
        FAKE_SECRETS_YAML_FILE,
        api_request_session=mock_request_session_rate_limit_error,
        access_token_fetcher_session=mock_access_token_fetcher_session,
        api_rate_limit_wait_strategy=api_client.ApiRateLimitWaitStrategy.WAIT_ONE_HOUR,
    )
    with pytest.raises(api_client.ApiRateLimitError):
        request.fetch(
            api_client.TiktokRequest(query={}, start_date=None, end_date=None),
            max_api_rate_limit_retries=num_retries
        )
    # Confirm that code retried the post request and json extraction twice (ie retried once after
    # the decode error before the exception is re-raised)
    assert mock_request_session_rate_limit_error.post.call_count == num_retries
    assert (
        mock_request_session_rate_limit_error.post.return_value.json.call_count == num_retries
    )
    # Sleep will be called once less than num_retries because it is not called after last retry
    assert mock_sleep.call_count == num_retries - 1 
    assert mock_sleep.mock_calls == [
            call(3600.0),
            call(3600.0),
            call(3600.0),
            call(3600.0),
            ]

@unittest.mock.patch('tenacity.nap.time.sleep')
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
                max_api_rate_limit_retries=num_retries
            )
        # Confirm that code retried the post request and json extraction twice (ie retried once after
        # the decode error before the exception is re-raised)
        assert mock_request_session_rate_limit_error.post.call_count == num_retries
        assert (
            mock_request_session_rate_limit_error.post.return_value.json.call_count == num_retries
        )
        # Sleep will be called once less than num_retries because it is not called after last retry
        assert mock_sleep.call_count == num_retries - 1
        assert mock_sleep.mock_calls == [
                call(expected_sleep_duration),
                call(expected_sleep_duration),
                call(expected_sleep_duration),
                call(expected_sleep_duration),
                ]
