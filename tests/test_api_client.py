import copy
import itertools
import json
import re
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, PropertyMock, call

import pendulum
import pytest
import requests
from sqlalchemy.orm import Session

from tests.test_utils import (
    all_crawls,
    all_videos,
)
from tiktok_research_api_helper import api_client, query, utils

FAKE_SECRETS_YAML_FILE = Path("tests/testdata/fake_secrets.yaml")


@pytest.fixture
def mock_request_session():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json = Mock(side_effect=json.JSONDecodeError(msg="", doc="", pos=0))

    mock_session = Mock(autospec=requests.Session)
    mock_session.headers = {}
    mock_session.hooks = {"response": []}

    mock_session.post = Mock(return_value=mock_response)

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
    api_client.TikTokCredentials("client_id_1", "client_secret_1", "client_key_1")
    api_client.TikTokCredentials(123, "client_secret_1", "client_key_1")


def test_tiktok_credentials_any_value_missing_raises_value_error():
    with pytest.raises(ValueError):
        api_client.TikTokCredentials("", "", "")

    with pytest.raises(ValueError):
        api_client.TikTokCredentials("client_id_1", "client_secret_1", "")

    with pytest.raises(ValueError):
        api_client.TikTokCredentials("client_id_1", "", "client_key_1")

    with pytest.raises(ValueError):
        api_client.TikTokCredentials("", "client_secret_1", "client_key_1")

    with pytest.raises(ValueError):
        api_client.TikTokCredentials("", "", "")


def test_tiktok_api_request_client_empty_credentials_raises_value_error():
    with pytest.raises(TypeError):
        api_client.TikTokApiRequestClient(credentials=None)

    with pytest.raises(TypeError):
        api_client.TikTokApiRequestClient(credentials={})


@pytest.mark.parametrize("video_id_arg", [1, "1"])
def test_tiktok_comments_request(video_id_arg):
    req = api_client.TikTokCommentsRequest(video_id=video_id_arg)
    assert req.video_id == 1


def test_tiktok_api_request_client_from_credentials_file_factory(
    mock_request_session, mock_access_token_fetcher_session
):
    request = api_client.TikTokApiRequestClient.from_credentials_file(
        FAKE_SECRETS_YAML_FILE,
        api_request_session=mock_request_session,
        access_token_fetcher_session=mock_access_token_fetcher_session,
    )
    assert request._credentials == api_client.TikTokCredentials(
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
        request.fetch_videos(
            api_client.TikTokVideoRequest(query={}, start_date=None, end_date=None)
        )
    # Confirm that code retried the post request and json extraction twice (ie retried once after
    # the decode error before the exception is re-raised)
    assert mock_request_session_json_decoder_error.post.call_count == 2
    assert mock_request_session_json_decoder_error.post.return_value.json.call_count == 2
    mock_sleep.assert_called_once_with(0)


@pytest.mark.parametrize("num_retries", range(1, 6))
@unittest.mock.patch("tenacity.nap.time.sleep")
def test_tiktok_api_request_client_wait_one_hour_on_rate_limit_wait_strategy(
    mock_sleep,
    mock_request_session_rate_limit_error,
    mock_access_token_fetcher_session,
    num_retries,
):
    request = api_client.TikTokApiRequestClient.from_credentials_file(
        FAKE_SECRETS_YAML_FILE,
        api_request_session=mock_request_session_rate_limit_error,
        access_token_fetcher_session=mock_access_token_fetcher_session,
        api_rate_limit_wait_strategy=api_client.ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS,
        max_api_rate_limit_retries=num_retries,
    )
    with pytest.raises(api_client.ApiRateLimitError):
        request.fetch_videos(
            api_client.TikTokVideoRequest(query={}, start_date=None, end_date=None),
        )
    # Confirm that code retried the post request and json extraction twice (ie retried once after
    # the decode error before the exception is re-raised)
    assert mock_request_session_rate_limit_error.post.call_count == num_retries
    assert mock_request_session_rate_limit_error.post.return_value.json.call_count == num_retries
    # Sleep will be called once less than num_retries because it is not called after last retry
    expected_call_count = num_retries - 1
    assert mock_sleep.call_count == expected_call_count
    assert mock_sleep.mock_calls == [call(14400.0)] * expected_call_count


@pytest.mark.parametrize("num_retries", range(1, 6))
@unittest.mock.patch("tenacity.nap.time.sleep")
def test_tiktok_api_request_client_wait_til_next_utc_midnight_on_rate_limit_wait_strategy(
    mock_sleep,
    mock_request_session_rate_limit_error,
    mock_access_token_fetcher_session,
    num_retries,
):
    # Freeze time so that we can predict time til midnight
    with pendulum.travel(freeze=True):
        expected_sleep_duration = (pendulum.tomorrow("UTC") - pendulum.now()).seconds
        request = api_client.TikTokApiRequestClient.from_credentials_file(
            FAKE_SECRETS_YAML_FILE,
            api_request_session=mock_request_session_rate_limit_error,
            access_token_fetcher_session=mock_access_token_fetcher_session,
            api_rate_limit_wait_strategy=api_client.ApiRateLimitWaitStrategy.WAIT_NEXT_UTC_MIDNIGHT,
            max_api_rate_limit_retries=num_retries,
        )
        with pytest.raises(api_client.ApiRateLimitError):
            request.fetch_videos(
                api_client.TikTokVideoRequest(query={}, start_date=None, end_date=None),
            )
        # Confirm that code retried the post request and json extraction twice (ie retried once
        # after the decode error before the exception is re-raised)
        assert mock_request_session_rate_limit_error.post.call_count == num_retries
        assert (
            mock_request_session_rate_limit_error.post.return_value.json.call_count == num_retries
        )
        # Sleep will be called once less than num_retries because it is not called after last retry
        expected_call_count = num_retries - 1
        assert mock_sleep.call_count == num_retries - 1
        assert mock_sleep.mock_calls == [call(expected_sleep_duration)] * expected_call_count


@pytest.fixture
def mock_tiktok_video_responses(testdata_api_videos_response_json):
    first_page = copy.deepcopy(testdata_api_videos_response_json)

    second_page = copy.deepcopy(first_page)
    # Emulate API incrementing cursor by number of previous results
    second_page["data"]["cursor"] = 200
    # Modifiy video IDs so that database treats them as distinct
    for video in second_page["data"]["videos"]:
        video["id"] += 1

    last_page = copy.deepcopy(second_page)
    # Emulate API indicating this is the last page of results
    last_page["data"]["has_more"] = False
    # Emulate API incrementing cursor by number of previous results
    last_page["data"]["cursor"] = 300
    # Modifiy video IDs so that database treats them as distinct
    for video in last_page["data"]["videos"]:
        video["id"] += 1

    return [
        api_client.TikTokVideoResponse(
            data=first_page["data"],
            videos=first_page["data"]["videos"],
            error=first_page["error"],
        ),
        api_client.TikTokVideoResponse(
            data=second_page["data"],
            videos=second_page["data"]["videos"],
            error=second_page["error"],
        ),
        api_client.TikTokVideoResponse(
            data=last_page["data"],
            videos=last_page["data"]["videos"],
            error=last_page["error"],
        ),
    ]


@pytest.fixture
def mock_tiktok_comments_response(testdata_api_comments_response_json):
    return api_client.TikTokCommentsResponse(
        data=testdata_api_comments_response_json["data"],
        error=testdata_api_comments_response_json["error"],
        comments=testdata_api_comments_response_json["data"]["comments"],
    )


@pytest.fixture
def mock_user_info_ok_response_infinite_generator():
    data = {
        "data": {
            "bio_description": "my_bio",
            "is_verified": False,
            "likes_count": 27155089,
            "video_count": 44,
            "avatar_url": "https://some_cdn.com/my_avatar",
            "follower_count": 232,
            "following_count": 45,
            "display_name": "my nick name",
        },
        "error": {
            "code": "ok",
            "message": "",
            "log_id": "202207280326050102231031430C7E754E",
        },
    }
    return itertools.repeat(
        api_client.TikTokUserInfoResponse(data=data, error=data["error"], user_info=data)
    )


@pytest.fixture
def mock_tiktok_request_client(
    mock_tiktok_video_responses,
    mock_tiktok_comments_response,
    mock_user_info_ok_response_infinite_generator,
):
    mock_request_client = MagicMock(autospec=api_client.TikTokApiRequestClient)
    mock_request_client.fetch_videos = Mock(side_effect=mock_tiktok_video_responses)
    mock_request_client.fetch_user_info = Mock(
        side_effect=mock_user_info_ok_response_infinite_generator
    )
    mock_request_client.fetch_comments = Mock(return_value=mock_tiktok_comments_response)
    # Make property return fetch mock call count (emulating real behavior)
    type(mock_request_client).num_api_requests_sent = PropertyMock(
        side_effect=lambda: sum(
            [
                mock_request_client.fetch_videos.call_count,
                mock_request_client.fetch_user_info.call_count,
                mock_request_client.fetch_comments.call_count,
            ]
        )
    )
    return mock_request_client


@pytest.fixture
def basic_acquisition_config():
    return api_client.ApiClientConfig(
        video_query=query.generate_query(include_any_hashtags="test1,test2"),
        start_date=pendulum.parse("20240601"),
        end_date=pendulum.parse("20240601"),
        engine=None,
        api_credentials_file=None,
    )


@pytest.fixture
def expected_fetch_video_calls(basic_acquisition_config, mock_tiktok_video_responses):
    return [
        call(
            api_client.TikTokVideoRequest(
                query=basic_acquisition_config.video_query,
                start_date=utils.date_to_tiktok_str_format(basic_acquisition_config.start_date),
                end_date=utils.date_to_tiktok_str_format(basic_acquisition_config.end_date),
                max_count=basic_acquisition_config.max_count,
                is_random=False,
                cursor=None,
                search_id=None,
            )
        ),
        call(
            api_client.TikTokVideoRequest(
                query=basic_acquisition_config.video_query,
                start_date=utils.date_to_tiktok_str_format(basic_acquisition_config.start_date),
                end_date=utils.date_to_tiktok_str_format(basic_acquisition_config.end_date),
                max_count=basic_acquisition_config.max_count,
                is_random=False,
                cursor=basic_acquisition_config.max_count,
                search_id=mock_tiktok_video_responses[-1].data["search_id"],
            )
        ),
        call(
            api_client.TikTokVideoRequest(
                query=basic_acquisition_config.video_query,
                start_date=utils.date_to_tiktok_str_format(basic_acquisition_config.start_date),
                end_date=utils.date_to_tiktok_str_format(basic_acquisition_config.end_date),
                max_count=basic_acquisition_config.max_count,
                is_random=False,
                cursor=basic_acquisition_config.max_count * 2,
                search_id=mock_tiktok_video_responses[-1].data["search_id"],
            )
        ),
    ]


def test_tiktok_user_info_response_as_json():
    assert api_client.TikTokUserInfoRequest("karl").as_json() == '{"username": "karl"}'


def test_tiktok_api_client_api_results_iter(
    basic_acquisition_config,
    mock_tiktok_request_client,
    mock_tiktok_video_responses,
    expected_fetch_video_calls,
):
    client = api_client.TikTokApiClient(
        request_client=mock_tiktok_request_client, config=basic_acquisition_config
    )
    for i, response in enumerate(client.api_results_iter()):
        assert response.videos == mock_tiktok_video_responses[i].videos
        assert response.crawl.has_more == (i < 2), f"hash_more: {response.crawl.has_more}, i: {i}"
        assert response.crawl.cursor == basic_acquisition_config.max_count * (i + 1)

    assert mock_tiktok_request_client.fetch_videos.call_count == len(mock_tiktok_video_responses)
    assert mock_tiktok_request_client.fetch_videos.mock_calls == expected_fetch_video_calls


@pytest.fixture
def mock_tiktok_request_client_raises_max_api_requests_reached_error(
    mock_tiktok_video_responses, mock_user_info_ok_response_infinite_generator
):
    mock_request_client = MagicMock(autospec=api_client.TikTokApiRequestClient)
    side_effects = [
        mock_tiktok_video_responses[0],
        mock_tiktok_video_responses[1],
        api_client.MaxApiRequestsReachedError,
    ]
    mock_request_client.fetch_videos = Mock(side_effect=side_effects)
    mock_request_client.fetch_user_info = Mock(side_effect=side_effects)
    mock_request_client.fetch_comments = Mock(side_effect=side_effects)
    return mock_request_client


def test_tiktok_api_client_api_results_iter_max_api_requests_limit_reached(
    basic_acquisition_config,
    mock_tiktok_request_client_raises_max_api_requests_reached_error,
    mock_tiktok_video_responses,
    expected_fetch_video_calls,
):
    basic_acquisition_config.max_api_requests = len(mock_tiktok_video_responses) - 1
    client = api_client.TikTokApiClient(
        request_client=mock_tiktok_request_client_raises_max_api_requests_reached_error,
        config=basic_acquisition_config,
    )
    for i, response in enumerate(client.api_results_iter()):
        assert i + 1 <= basic_acquisition_config.max_api_requests
        assert response.videos == mock_tiktok_video_responses[i].videos
        assert response.crawl.has_more
        assert response.crawl.cursor == basic_acquisition_config.max_count * (i + 1)

    # Call count will be max_requests + 1 because client has to call func to raise
    # MaxApiRequestsReachedError
    assert (
        mock_tiktok_request_client_raises_max_api_requests_reached_error.fetch_videos.call_count
        == basic_acquisition_config.max_api_requests + 1
    )


def test_tiktok_api_client_fetch_all(
    basic_acquisition_config,
    mock_tiktok_request_client,
    mock_tiktok_video_responses,
    expected_fetch_video_calls,
):
    client = api_client.TikTokApiClient(
        request_client=mock_tiktok_request_client, config=basic_acquisition_config
    )

    fetch_result = client.fetch_all()

    assert fetch_result.videos == list(
        itertools.chain.from_iterable([r.videos for r in mock_tiktok_video_responses])
    )
    assert not fetch_result.crawl.has_more
    assert fetch_result.crawl.cursor == basic_acquisition_config.max_count * len(
        mock_tiktok_video_responses
    )
    assert mock_tiktok_request_client.fetch_videos.call_count == len(mock_tiktok_video_responses)
    assert mock_tiktok_request_client.fetch_videos.mock_calls == expected_fetch_video_calls


def test_tiktok_api_client_fetch_all_rejects_positional_arg(
    basic_acquisition_config,
    mock_tiktok_request_client,
):
    client = api_client.TikTokApiClient(
        request_client=mock_tiktok_request_client, config=basic_acquisition_config
    )

    bad_positional_arg = "bad positional arg"

    with pytest.raises(
        ValueError, match=re.escape("This function does not allow any positional arguments")
    ):
        client.fetch_all(bad_positional_arg)


def test_tiktok_api_client_fetch_all_do_not_store_after_each_response(
    test_database_engine,
    basic_acquisition_config,
    mock_tiktok_request_client,
    mock_tiktok_video_responses,
    expected_fetch_video_calls,
):
    basic_acquisition_config.engine = test_database_engine
    client = api_client.TikTokApiClient(
        request_client=mock_tiktok_request_client, config=basic_acquisition_config
    )

    fetch_result = client.fetch_all(store_results_after_each_response=False)

    assert fetch_result.videos == list(
        itertools.chain.from_iterable([r.videos for r in mock_tiktok_video_responses])
    )
    assert not fetch_result.crawl.has_more
    assert fetch_result.crawl.cursor == basic_acquisition_config.max_count * len(
        mock_tiktok_video_responses
    )
    assert mock_tiktok_request_client.fetch_videos.call_count == len(mock_tiktok_video_responses)
    assert mock_tiktok_request_client.fetch_videos.mock_calls == expected_fetch_video_calls
    # Confirm nothing put to database
    with Session(test_database_engine) as session:
        assert all_crawls(session) == []
        assert all_videos(session) == []


def assert_has_expected_crawl_and_videos_in_database(
    database_engine, fetch_result, tiktok_responses, acquisition_config
):
    with Session(database_engine) as session:
        crawls = all_crawls(session)
        assert len(crawls) == 1
        crawl = crawls[0]
        assert crawl.id == fetch_result.crawl.id
        assert crawl.cursor == len(tiktok_responses) * acquisition_config.max_count
        assert crawl.query == json.dumps(
            acquisition_config.video_query, cls=query.VideoQueryJSONEncoder
        )
        videos = all_videos(session)
        assert len(videos) == len(tiktok_responses) * len(tiktok_responses[0].videos)
        assert len(videos) == len(fetch_result.videos)


def test_tiktok_api_client_fetch_all_store_after_each_response(
    test_database_engine,
    basic_acquisition_config,
    mock_tiktok_request_client,
    mock_tiktok_video_responses,
    expected_fetch_video_calls,
):
    basic_acquisition_config.engine = test_database_engine
    client = api_client.TikTokApiClient(
        request_client=mock_tiktok_request_client, config=basic_acquisition_config
    )

    fetch_result = client.fetch_all(store_results_after_each_response=True)

    assert fetch_result.videos == list(
        itertools.chain.from_iterable([r.videos for r in mock_tiktok_video_responses])
    )
    assert not fetch_result.crawl.has_more
    assert fetch_result.crawl.cursor == basic_acquisition_config.max_count * len(
        mock_tiktok_video_responses
    )
    assert mock_tiktok_request_client.fetch_videos.call_count == len(mock_tiktok_video_responses)
    assert mock_tiktok_request_client.fetch_videos.mock_calls == expected_fetch_video_calls
    assert_has_expected_crawl_and_videos_in_database(
        database_engine=test_database_engine,
        fetch_result=fetch_result,
        tiktok_responses=mock_tiktok_video_responses,
        acquisition_config=basic_acquisition_config,
    )


def test_tiktok_api_client_store_fetch_result(
    test_database_engine,
    basic_acquisition_config,
    mock_tiktok_request_client,
    mock_tiktok_video_responses,
):
    basic_acquisition_config.engine = test_database_engine
    client = api_client.TikTokApiClient(
        request_client=mock_tiktok_request_client, config=basic_acquisition_config
    )
    fetch_result = client.fetch_and_store_all()
    client.store_fetch_result(fetch_result)
    assert_has_expected_crawl_and_videos_in_database(
        database_engine=test_database_engine,
        fetch_result=fetch_result,
        tiktok_responses=mock_tiktok_video_responses,
        acquisition_config=basic_acquisition_config,
    )


def test_tiktok_api_client_fetch_and_store_all(
    test_database_engine,
    basic_acquisition_config,
    mock_tiktok_request_client,
    mock_tiktok_video_responses,
):
    basic_acquisition_config.engine = test_database_engine
    client = api_client.TikTokApiClient(
        request_client=mock_tiktok_request_client, config=basic_acquisition_config
    )
    fetch_result = client.fetch_and_store_all()
    assert_has_expected_crawl_and_videos_in_database(
        database_engine=test_database_engine,
        fetch_result=fetch_result,
        tiktok_responses=mock_tiktok_video_responses,
        acquisition_config=basic_acquisition_config,
    )


@pytest.mark.parametrize(
    ("fetch_comments", "fetch_user_info"),
    # All 4 possible combos of True, False matrix
    itertools.product((True, False), repeat=2),
)
def test_tiktok_api_client_api_results_iter_fetches_comments_and_or_user_info_if_configured(
    basic_acquisition_config,
    mock_tiktok_request_client,
    mock_tiktok_video_responses,
    expected_fetch_video_calls,
    fetch_comments,
    fetch_user_info,
):
    basic_acquisition_config.fetch_comments = fetch_comments
    basic_acquisition_config.fetch_user_info = fetch_user_info

    client = api_client.TikTokApiClient(
        request_client=mock_tiktok_request_client, config=basic_acquisition_config
    )

    client.fetch_all()

    assert mock_tiktok_request_client.fetch_videos.call_count == len(mock_tiktok_video_responses)
    assert mock_tiktok_request_client.fetch_comments.call_count == (
        sum([len(response.videos) for response in mock_tiktok_video_responses])
        if fetch_comments
        else 0
    )
    num_unique_usernames = len(
        {video["username"] for response in mock_tiktok_video_responses for video in response.videos}
    )
    assert mock_tiktok_request_client.fetch_user_info.call_count == (
        num_unique_usernames if fetch_user_info else 0
    )


def test_get_unfetched_attribute_identifiers_from_api_video_response_usernames(
    mock_tiktok_video_responses,
):
    fetched_usernames = set()
    expected_usernames = {x.get("username") for x in mock_tiktok_video_responses[0].videos}

    assert (
        api_client.get_unfetched_attribute_identifiers_from_api_video_response(
            mock_tiktok_video_responses[0], "username", fetched_usernames
        )
        == expected_usernames
    )

    # Now mark a few usernames fetched, and remove those from expected_usernames
    fetched_usernames.update({expected_usernames.pop() for x in range(5)})

    assert (
        api_client.get_unfetched_attribute_identifiers_from_api_video_response(
            mock_tiktok_video_responses[0], "username", fetched_usernames
        )
        == expected_usernames
    )

    # Now mark all usernames from expected to fetched
    fetched_usernames.update(expected_usernames)
    expected_usernames.clear()

    # Since mock_titkok_response[1] has the same usernames as mock_titkok_response[0] this should
    # return an empty set
    assert (
        api_client.get_unfetched_attribute_identifiers_from_api_video_response(
            mock_tiktok_video_responses[1], "username", fetched_usernames
        )
        == set()
    )


def test_get_unfetched_attribute_identifiers_from_api_video_response_video_ids(
    mock_tiktok_video_responses,
):
    fetched_ids = set()
    expected_ids = {x.get("id") for x in mock_tiktok_video_responses[0].videos}

    assert (
        api_client.get_unfetched_attribute_identifiers_from_api_video_response(
            mock_tiktok_video_responses[0], "id", fetched_ids
        )
        == expected_ids
    )

    # Now mark a few ids fetched, and remove those from expected_ids
    fetched_ids.update({expected_ids.pop() for x in range(5)})

    assert (
        api_client.get_unfetched_attribute_identifiers_from_api_video_response(
            mock_tiktok_video_responses[0], "id", fetched_ids
        )
        == expected_ids
    )

    # Now mark all id from expected to fetched
    fetched_ids.update(expected_ids)
    expected_ids.clear()

    # Since mock_titkok_response[1] has the different ids as mock_titkok_response[0] this should
    # return all ids in mock_titkok_response[1]
    assert api_client.get_unfetched_attribute_identifiers_from_api_video_response(
        mock_tiktok_video_responses[1], "id", fetched_ids
    ) == {x.get("id") for x in mock_tiktok_video_responses[1].videos}


@pytest.mark.parametrize("max_api_requests", range(0, 5))
def test_tiktok_request_client_fetch_videos_raises_max_api_requests_reached_error_correctly(
    mock_request_session,
    mock_access_token_fetcher_session,
    monkeypatch,
    max_api_requests,
):
    request_client = api_client.TikTokApiRequestClient.from_credentials_file(
        FAKE_SECRETS_YAML_FILE,
        api_request_session=mock_request_session,
        access_token_fetcher_session=mock_access_token_fetcher_session,
        max_api_requests=max_api_requests,
    )
    request = api_client.TikTokVideoRequest(query=None, start_date=None, end_date=None)
    assert request_client.num_api_requests_sent == 0

    with monkeypatch.context() as m:
        m.setattr(api_client, "_parse_video_response", lambda x: x)
        for x in range(1, max_api_requests + 1):
            request_client.fetch_videos(request)
            assert request_client.num_api_requests_sent == x

        with pytest.raises(api_client.MaxApiRequestsReachedError):
            request_client.fetch_videos(request)
        assert request_client.num_api_requests_sent == max_api_requests


@pytest.mark.parametrize("max_api_requests", range(0, 5))
def test_tiktok_request_client_fetch_comments_raises_max_api_requests_reached_error_correctly(
    mock_request_session,
    mock_access_token_fetcher_session,
    monkeypatch,
    max_api_requests,
):
    request_client = api_client.TikTokApiRequestClient.from_credentials_file(
        FAKE_SECRETS_YAML_FILE,
        api_request_session=mock_request_session,
        access_token_fetcher_session=mock_access_token_fetcher_session,
        max_api_requests=max_api_requests,
    )
    request = api_client.TikTokCommentsRequest(video_id=1)
    assert request_client.num_api_requests_sent == 0

    with monkeypatch.context() as m:
        m.setattr(api_client, "_parse_comments_response", lambda x: x)
        for x in range(1, max_api_requests + 1):
            request_client.fetch_comments(request)
            assert request_client.num_api_requests_sent == x

        with pytest.raises(api_client.MaxApiRequestsReachedError):
            request_client.fetch_comments(request)
        assert request_client.num_api_requests_sent == max_api_requests


@pytest.mark.parametrize("max_api_requests", range(0, 5))
def test_tiktok_request_client_fetch_user_info_raises_max_api_requests_reached_error_correctly(
    mock_request_session,
    mock_access_token_fetcher_session,
    monkeypatch,
    max_api_requests,
):
    request_client = api_client.TikTokApiRequestClient.from_credentials_file(
        FAKE_SECRETS_YAML_FILE,
        api_request_session=mock_request_session,
        access_token_fetcher_session=mock_access_token_fetcher_session,
        max_api_requests=max_api_requests,
    )
    request = api_client.TikTokUserInfoRequest(username="a")
    assert request_client.num_api_requests_sent == 0

    with monkeypatch.context() as m:
        m.setattr(
            api_client,
            "_parse_user_info_response",
            lambda x: api_client.TikTokUserInfoResponse(
                data=None, error=None, user_info={"username": "a"}
            ),
        )

        for x in range(1, max_api_requests + 1):
            request_client.fetch_user_info(request)
            assert request_client.num_api_requests_sent == x

        with pytest.raises(api_client.MaxApiRequestsReachedError):
            request_client.fetch_user_info(request)
        assert request_client.num_api_requests_sent == max_api_requests


def test_tiktok_request_client_mixed_fetch_raises_max_api_requests_reached_error_correctly(
    mock_request_session,
    mock_access_token_fetcher_session,
    monkeypatch,
):
    request_client = api_client.TikTokApiRequestClient.from_credentials_file(
        FAKE_SECRETS_YAML_FILE,
        api_request_session=mock_request_session,
        access_token_fetcher_session=mock_access_token_fetcher_session,
        max_api_requests=3,
    )

    video_request = api_client.TikTokVideoRequest(query=None, start_date=None, end_date=None)
    comment_request = api_client.TikTokCommentsRequest(video_id=1)
    user_info_request = api_client.TikTokUserInfoRequest(username="a")
    assert request_client.num_api_requests_sent == 0

    with monkeypatch.context() as m:
        m.setattr(api_client, "_parse_video_response", lambda x: x)
        m.setattr(api_client, "_parse_comments_response", lambda x: x)
        m.setattr(
            api_client,
            "_parse_user_info_response",
            lambda x: api_client.TikTokUserInfoResponse(
                data=None, error=None, user_info={"username": "a"}
            ),
        )

        request_client.fetch_videos(video_request)
        request_client.fetch_comments(comment_request)
        request_client.fetch_user_info(user_info_request)
        assert request_client.num_api_requests_sent == 3

        # Confirm that any fetch type is raise MaxApiRequestsReachedError
        with pytest.raises(api_client.MaxApiRequestsReachedError):
            request_client.fetch_videos(video_request)
        assert request_client.num_api_requests_sent == 3

        with pytest.raises(api_client.MaxApiRequestsReachedError):
            request_client.fetch_comments(comment_request)
        assert request_client.num_api_requests_sent == 3

        with pytest.raises(api_client.MaxApiRequestsReachedError):
            request_client.fetch_user_info(user_info_request)
        assert request_client.num_api_requests_sent == 3
