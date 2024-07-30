from __future__ import annotations

import enum
import json
import logging
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import attrs
import certifi
import pendulum
import requests as rq
import tenacity
import yaml
from sqlalchemy import Engine

from tiktok_research_api_helper import utils
from tiktok_research_api_helper.models import (
    Crawl,
    upsert_comments,
    upsert_user_info,
    upsert_videos,
)
from tiktok_research_api_helper.query import VideoQuery, VideoQueryJSONEncoder

ALL_VIDEO_DATA_URL = "https://open.tiktokapis.com/v2/research/video/query/?fields=id,video_description,create_time,region_code,share_count,view_count,like_count,comment_count,music_id,hashtag_names,username,effect_ids,voice_to_text,playlist_id"
ALL_USER_INFO_DATA_URL = "https://open.tiktokapis.com/v2/research/user/info/?fields=display_name,bio_description,avatar_url,is_verified,follower_count,following_count,likes_count,video_count"
ALL_COMMENT_DATA_URL = "https://open.tiktokapis.com/v2/research/video/comment/list/?fields=id,like_count,create_time,text,video_id,parent_comment_id"

SEARCH_ID_INVALID_ERROR_MESSAGE_REGEX = re.compile(r"Search Id \d+ is invalid or expired")

INVALID_SEARCH_ID_ERROR_RETRY_WAIT_BASE = 5
INVALID_SEARCH_ID_ERROR_MAX_NUM_RETRIES = 5
# TikTok research API only allows fetching top 1000 comments. https://developers.tiktok.com/doc/research-api-specs-query-video-comments
MAX_COMMENTS_CURSOR = 999
API_ERROR_RETRY_LIMIT = 5
API_ERROR_RETRY_MAX_WAIT = timedelta(minutes=2).total_seconds()

DAILY_API_REQUEST_QUOTA = 1000


class ApiRateLimitError(Exception):
    pass


class InvalidRequestError(Exception):
    def __init__(self, message, response, error_json=None):
        super().__init__(message, response)
        self.error_json = error_json


class InvalidSearchIdError(InvalidRequestError):
    pass


class InvalidCountOrCursorError(InvalidRequestError):
    pass


class InvalidUsernameError(InvalidRequestError):
    pass


class RefusedUsernameError(InvalidRequestError):
    """Raised when API says it 'cannot return this user's information'"""

    pass


class ApiServerError(Exception):
    """Raised when API responds 500"""

    pass


class MaxApiRequestsReachedError(Exception):
    """Raised when TikTokApiRequestClient attempts a request to the API that would exceed the
    configured maxmimum allowd api requests"""

    pass


def field_is_not_empty(instance, attribute, value):
    if not value:
        raise ValueError(f"{instance.__class__.__name__}: {attribute.name} cannot be empty")


class ApiRateLimitWaitStrategy(enum.StrEnum):
    WAIT_FOUR_HOURS = enum.auto()
    WAIT_NEXT_UTC_MIDNIGHT = enum.auto()


@attrs.define
class TikTokCredentials:
    client_id: str = attrs.field(
        validator=[attrs.validators.instance_of((str, int)), field_is_not_empty]
    )
    client_secret: str = attrs.field(
        validator=[attrs.validators.instance_of(str), field_is_not_empty]
    )
    client_key: str = attrs.field(validator=[attrs.validators.instance_of(str), field_is_not_empty])


@attrs.define
class TikTokResponse:
    data: Mapping[str, Any]
    error: Mapping[str, Any]  # ErrorStructV2 from API


@attrs.define
class TikTokVideoResponse(TikTokResponse):
    videos: Sequence[Any]


@attrs.define
class TikTokUserInfoResponse(TikTokResponse):
    username: str
    user_info: Mapping[str, str]


@attrs.define
class TikTokCommentsResponse(TikTokResponse):
    comments: Sequence[any]


@attrs.define
class TikTokApiClientFetchResult:
    videos: Sequence[Any]
    user_info: Sequence[Any] | None
    comments: Sequence[Any] | None
    crawl: Crawl


def video_query_to_json(video_query: VideoQuery) -> str:
    if isinstance(video_query, VideoQuery | Mapping):
        return json.dumps(video_query, cls=VideoQueryJSONEncoder)
    return video_query


@attrs.define
class VideoQueryConfig:
    query: str = attrs.field(
        converter=video_query_to_json, validator=attrs.validators.instance_of(str)
    )
    start_date: datetime = attrs.field(validator=attrs.validators.instance_of((date, datetime)))
    end_date: datetime = attrs.field(validator=attrs.validators.instance_of((date, datetime)))
    # WARNING: Fetching comments can greatly increase API quota usage. use with care.
    fetch_comments: bool = False
    fetch_user_info: bool = False
    max_count: int = 100
    crawl_tags: list[str] | None = None


@attrs.define
class ApiClientConfig:
    api_credentials_file: Path
    engine: Engine | None = attrs.field(
        default=None, validator=attrs.validators.optional(attrs.validators.instance_of(Engine))
    )
    raw_responses_output_dir: Path | None = None
    api_rate_limit_wait_strategy: ApiRateLimitWaitStrategy = attrs.field(
        default=ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS,
        validator=attrs.validators.instance_of(ApiRateLimitWaitStrategy),  # type: ignore - Attrs overload
    )
    # Limit on number of API requests. None is no limit. Otherwise client will stop, regardless of
    # whether API indicates has_more, if it has made this many requests. Does not include retries
    # due to error/timeout
    max_api_requests: int | None = attrs.field(
        default=None,
        validator=attrs.validators.optional(
            [attrs.validators.instance_of(int), attrs.validators.gt(0)]
        ),
    )
    # None indicates no limit (ie retry indefinitely)
    max_api_rate_limit_retries: int | None = None
    # raise error when api responds with server error (ie 500) even after multiple retries. NOTE:
    # If this is false, you can see if results are complete from lastest crawl.has_more (ie if True,
    # results not fully delivered)
    raise_error_on_persistent_api_server_error: bool = False


@attrs.define
class TikTokVideoRequest:
    """
    A TikTokVideoRequest.

    The start date is inclusive but the end date is NOT.
    """

    query: str = attrs.field(
        converter=video_query_to_json, validator=attrs.validators.instance_of(str)
    )
    start_date: str
    end_date: str  # The end date is NOT inclusive!
    max_count: int = 100
    is_random: bool = False

    cursor: int | None = None
    search_id: str | None = None

    @classmethod
    def from_config(cls, config: VideoQueryConfig, **kwargs) -> TikTokVideoRequest:
        return cls(
            query=config.query,
            max_count=config.max_count,
            start_date=utils.date_to_tiktok_str_format(config.start_date),
            end_date=utils.date_to_tiktok_str_format(config.end_date),
            **kwargs,
        )

    def as_json(self, indent=None):
        return json.dumps(
            attrs.asdict(self, value_serializer=json_query_dict_serializer), indent=indent
        )


def json_query_dict_serializer(inst, field, value):
    # Since json_query is already encoded as JSON, we need to decode it to make a dict.
    if field == attrs.fields(TikTokVideoRequest).query:
        return json.loads(value)
    return value


@attrs.define
class TikTokUserInfoRequest:
    """
    A request for User Info TikTok research API.
    """

    username: str

    def as_json(self, indent=None):
        return json.dumps(attrs.asdict(self), indent=indent)


@attrs.define
class TikTokCommentsRequest:
    """
    A TikTokCommentsRequest.
    """

    video_id: str = attrs.field(validator=attrs.validators.instance_of(int), converter=int)
    max_count: int = attrs.field(
        default=100, validator=attrs.validators.instance_of(int), converter=int
    )
    cursor: int | None = attrs.field(
        default=None, validator=attrs.validators.optional(attrs.validators.instance_of(int))
    )

    def as_json(self, indent=None):
        return json.dumps(attrs.asdict(self), indent=indent)


def response_is_ok(tiktok_response: TikTokResponse) -> bool:
    return tiktok_response.error.get("code") == "ok"


def retry_json_decoding_error_once(
    retry_state,
):
    exception = retry_state.outcome.exception()

    # Retry once if JSON decoding response fails
    if isinstance(exception, rq.exceptions.JSONDecodeError | json.JSONDecodeError):
        return retry_state.attempt_number <= 1

    return None


def retry_invalid_search_id_error(
    retry_state,
):
    exception = retry_state.outcome.exception()

    # Workaround API bug where valid search ID (ie the one the API just returned) is rejected as
    # invalid.
    if isinstance(exception, InvalidSearchIdError | InvalidCountOrCursorError):
        return retry_state.attempt_number <= INVALID_SEARCH_ID_ERROR_MAX_NUM_RETRIES

    return None


def retry_api_rate_limit_error_indefintely(
    retry_state,
):
    exception = retry_state.outcome.exception()
    # Retry API rate lmiit errors indefinitely.
    if isinstance(exception, ApiRateLimitError):
        return True

    return None


def search_id_invalid_error_wait(retry_state):
    exception = retry_state.outcome.exception()
    # Wait in case API needs a few seconds to consider search ID valid.
    if isinstance(exception, InvalidSearchIdError | InvalidCountOrCursorError):
        return retry_state.attempt_number * INVALID_SEARCH_ID_ERROR_RETRY_WAIT_BASE

    return 0


def get_api_rate_limit_wait_strategy(
    api_rate_limit_wait_strategy: ApiRateLimitWaitStrategy,
):
    if api_rate_limit_wait_strategy == ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS:
        return api_rate_limi_wait_four_hours
    if api_rate_limit_wait_strategy == ApiRateLimitWaitStrategy.WAIT_NEXT_UTC_MIDNIGHT:
        return api_rate_limi_wait_until_next_utc_midnight

    raise ValueError(f"Unknown wait strategy: {api_rate_limit_wait_strategy}")


def api_rate_limi_wait_until_next_utc_midnight(retry_state):
    exception = retry_state.outcome.exception()
    # If JSON decoding fails retry immediately
    if isinstance(exception, ApiRateLimitError):
        next_utc_midnight = pendulum.tomorrow("UTC")
        logging.warning(
            "Response indicates rate limit exceeded: %r\n"
            "Sleeping until next UTC midnight: %s (local time %s). Will resume in approx %s",
            exception,
            next_utc_midnight,
            next_utc_midnight.in_tz("local"),
            next_utc_midnight.diff_for_humans(pendulum.now("local"), absolute=True),
        )
        return (next_utc_midnight - pendulum.now()).seconds

    return 0


def api_rate_limi_wait_four_hours(
    retry_state,
):
    exception = retry_state.outcome.exception()
    if isinstance(exception, ApiRateLimitError):
        logging.warning(
            "Response indicates rate limit exceeded: %r\nSleeping four hours before trying again.",
            exception,
        )
        return timedelta(hours=4).seconds

    return 0


@attrs.define
class TikTokApiRequestClient:
    """
    Class for making authenticated requests to the TikTok research API and getting a parsed
    response.
    """

    _credentials: TikTokCredentials = attrs.field(
        validator=[attrs.validators.instance_of(TikTokCredentials), field_is_not_empty],
        # Attrs removes underscores from field names but the static type checker doesn't know that
        alias="credentials",
    )
    _access_token_fetcher_session: rq.Session = attrs.field(
        default=None, kw_only=True, converter=attrs.converters.default_if_none(factory=rq.Session)
    )
    _api_request_session: rq.Session = attrs.field(
        default=None, kw_only=True, converter=attrs.converters.default_if_none(factory=rq.Session)
    )
    _raw_responses_output_dir: Path | None = attrs.field(
        default=None, validator=attrs.validators.optional(attrs.validators.instance_of(Path))
    )
    _api_rate_limit_wait_strategy: ApiRateLimitWaitStrategy = attrs.field(
        default=ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS,
        validator=attrs.validators.instance_of(ApiRateLimitWaitStrategy),  # type: ignore - Attrs overload
    )
    _num_api_requests_sent: int = attrs.field(
        default=0, kw_only=True, validator=attrs.validators.instance_of(int)
    )
    # None indicates no limit (ie retry indefinitely)
    _max_api_rate_limit_retries: int | None = attrs.field(
        default=None, validator=attrs.validators.optional(attrs.validators.instance_of(int))
    )
    _max_api_requests: int | None = attrs.field(
        default=None, validator=attrs.validators.optional(attrs.validators.instance_of(int))
    )

    @classmethod
    def from_credentials_file(cls, credentials_file: Path, **kwargs) -> TikTokApiRequestClient:
        with credentials_file.open("r") as f:
            dict_credentials = yaml.load(f, Loader=yaml.FullLoader)

        return cls(
            **kwargs,
            credentials=TikTokCredentials(**dict_credentials),
        )

    @property
    def num_api_requests_sent(self):
        return self._num_api_requests_sent

    def reset_num_requests(self):
        self._num_api_requests_sent = 0

    def __attrs_post_init__(self):
        self._configure_request_sessions()

    def _get_client_access_token(
        self,
        grant_type: str = "client_credentials",
    ) -> str:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Cache-Control": "no-cache",
        }

        data = {
            "client_key": self._credentials.client_key,
            "client_secret": self._credentials.client_secret,
            "grant_type": grant_type,
        }

        response = self._access_token_fetcher_session.post(
            "https://open.tiktokapis.com/v2/oauth/token/", headers=headers, data=data
        )
        if not response.ok:
            logging.error("Problem with access token response: %s", response)

        try:
            access_data = response.json()
        except rq.exceptions.JSONDecodeError as e:
            logging.info(
                "Access token raw response: %s\n%s\n%s",
                response.status_code,
                response.headers,
                response.text,
            )
            raise e
        logging.info("Access token retrieval succeeded")
        logging.debug("Access token response: %s", access_data)

        return access_data["access_token"]

    def _configure_request_sessions(self):
        """Gets access token for authorization, sets token in headers for all requests, and
        registers a hook to refresh token when response indicates it has expired. Configures access
        token fetcher and api fetcher sessions to verify certs using certifi."""
        self._access_token_fetcher_session.verify = certifi.where()

        token = self._get_client_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "text/plain",
        }

        self._api_request_session.headers.update(headers)
        self._api_request_session.hooks["response"].append(self._refresh_token)
        self._api_request_session.verify = certifi.where()

    def _refresh_token(
        self,
        r,
        *unused_args: Sequence | None,
        **unused_kwargs: Mapping[Any, Any] | None,
    ) -> rq.Response | None:
        # Adapted from https://stackoverflow.com/questions/37094419/python-requests-retry-request-after-re-authentication
        if r.status_code == 401:
            logging.info("Fetching new token as the previous token expired")

            token = self._get_client_access_token()
            self._api_request_session.headers.update({"Authorization": f"Bearer {token}"})

            r.request.headers["Authorization"] = self._api_request_session.headers["Authorization"]

            return self._api_request_session.send(r.request)

        return None

    def _store_response(self, response: rq.Response) -> None:
        if self._raw_responses_output_dir is None:
            raise ValueError("No output directory set")

        output_filename = self._raw_responses_output_dir / Path(
            str(pendulum.now("local").timestamp()) + ".json"
        )
        output_filename = output_filename.absolute()
        logging.info("Writing raw reponse to %s", output_filename)
        with output_filename.open("x") as f:
            f.write(response.text)

    def _fetch_retryer(self) -> tenacity.Retrying:
        """This retryer is for API level issues, ie Rate limit being hit, API bugs (like search ID
        and cursor being rejected as valid that are in fact valid and will be accepted on a
        subsequent request), responses which should be JSON but we cannot decode as JSON.

        There is another retryer (currently a tenacity.retry decorator on _post) which handles
        request level issues (like 500 errors, timeouts, etc).
        """
        if self._max_api_rate_limit_retries is None:
            stop_strategy = tenacity.stop_never
        else:
            stop_strategy = tenacity.stop_after_attempt(self._max_api_rate_limit_retries)

        return tenacity.Retrying(
            retry=tenacity.retry_any(
                retry_json_decoding_error_once,
                retry_invalid_search_id_error,
                retry_api_rate_limit_error_indefintely,
            ),
            wait=tenacity.wait_combine(
                search_id_invalid_error_wait,
                get_api_rate_limit_wait_strategy(
                    api_rate_limit_wait_strategy=self._api_rate_limit_wait_strategy
                ),
            ),
            stop=stop_strategy,
            before_sleep=tenacity.before_sleep_log(logging.getLogger(), logging.DEBUG),
            reraise=True,
        )

    def fetch_videos(self, request: TikTokVideoRequest) -> TikTokVideoResponse:
        return self._fetch_retryer()(self._fetch_videos_and_parse_response, request)

    def fetch_user_info(self, request: TikTokUserInfoRequest) -> TikTokUserInfoResponse:
        return self._fetch_retryer()(self._fetch_user_info_and_parse_response, request)

    def fetch_comments(self, request: TikTokCommentsRequest) -> TikTokCommentsResponse:
        return self._fetch_retryer()(self._fetch_comments_and_parse_response, request)

    def _fetch_videos_and_parse_response(self, request: TikTokVideoRequest) -> TikTokVideoResponse:
        return _parse_video_response(self._post(request, ALL_VIDEO_DATA_URL))

    def _fetch_user_info_and_parse_response(
        self, request: TikTokUserInfoRequest
    ) -> TikTokUserInfoResponse:
        parsed_response = _parse_user_info_response(
            username=request.username, response=self._post(request, ALL_USER_INFO_DATA_URL)
        )
        return parsed_response

    def _fetch_comments_and_parse_response(
        self, request: TikTokCommentsRequest
    ) -> TikTokCommentsResponse:
        return _parse_comments_response(self._post(request, ALL_COMMENT_DATA_URL))

    def max_api_requests_reached(self) -> bool:
        if self._max_api_requests is None:
            return False
        return self._num_api_requests_sent >= self._max_api_requests

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(API_ERROR_RETRY_LIMIT),
        wait=tenacity.wait_exponential(multiplier=2, min=3, max=API_ERROR_RETRY_MAX_WAIT),
        retry=tenacity.retry_if_exception_type((rq.RequestException, ApiServerError)),
        before_sleep=tenacity.before_sleep_log(logging.getLogger(), logging.DEBUG),
        reraise=True,
    )
    def _post(self, request: TikTokVideoRequest, url: str) -> rq.Response | None:
        if self.max_api_requests_reached():
            msg = (
                f"Refusing to send API request because it would exceed max requests limit: "
                f"{self._max_api_requests}.  This client has sent {self._num_api_requests_sent} "
                f"requests"
            )
            raise MaxApiRequestsReachedError(msg)
        data = request.as_json()
        logging.debug("Sending request with data: %s", data)

        response = self._api_request_session.post(url=url, data=data)
        logging.debug("%s\n%s", response, response.text)
        self._num_api_requests_sent += 1

        if self._raw_responses_output_dir is not None:
            self._store_response(response)

        if response.status_code == 200:
            return response

        if response.status_code == 429:
            msg = (
                f"Response indicates rate limit exceeded: {response!r}.\n"
                "num_api_requests_sent: {self.num_api_requests_sent}"
            )
            raise ApiRateLimitError(msg)

        if response.status_code == 400:
            try:
                response_json = response.json()
                response_json_error_message = response_json.get("error", {}).get("message", "")
                if SEARCH_ID_INVALID_ERROR_MESSAGE_REGEX.match(response_json_error_message):
                    raise InvalidSearchIdError(
                        f"{response!r} {response.text}",
                        response=response,
                        error_json=response_json.get("error", {}),
                    )
                if "is invalid: cannot find the user" in response_json_error_message:
                    raise InvalidUsernameError(
                        f"{response!r} {response.text}",
                        response=response,
                        error_json=response_json.get("error", {}),
                    )
                if "API cannot return this user's information" in response_json_error_message:
                    raise RefusedUsernameError(
                        f"{response!r} {response.text}",
                        response=response,
                        error_json=response_json.get("error", {}),
                    )
                if "Invalid count or cursor" in response_json_error_message:
                    raise InvalidCountOrCursorError(
                        f"{response!r} {response.text}",
                        response=response,
                        error_json=response_json.get("error", {}),
                    )

            except json.JSONDecodeError:
                logging.debug("Unable to JSON decode response data:\n%s", response.text)

            raise InvalidRequestError(f"{response!r} {response.text}", response=response)

        if response.status_code == 500:
            logging.info("API responded 500. This happens occasionally")
            raise ApiServerError(response.text)

        logging.warning(
            f"Request failed, status code {response.status_code} - text {response.text} - data "
            "{data}",
        )
        response.raise_for_status()
        # In case raise_for_status does not raise an exception we return None
        return None


def _parse_video_response(response: rq.Response) -> TikTokVideoResponse:
    response_json = _extract_response_json_or_raise_error(response)
    error_data = response_json.get("error")
    response_data_section = response_json.get("data", {})
    videos = response_data_section.get("videos", [])

    return TikTokVideoResponse(data=response_data_section, videos=videos, error=error_data)


def _parse_user_info_response(username: str, response: rq.Response) -> TikTokUserInfoResponse:
    response_json = _extract_response_json_or_raise_error(response)
    error_data = response_json.get("error")
    response_data_section = response_json.get("data")
    # API does not include username in response. for ease of use we add it.
    response_data_section["username"] = username

    return TikTokUserInfoResponse(
        username=username,
        user_info=response_data_section,
        data=response_data_section,
        error=error_data,
    )


def _parse_comments_response(response: rq.Response) -> TikTokCommentsResponse:
    response_json = _extract_response_json_or_raise_error(response)
    error_data = response_json.get("error")
    response_data_section = response_json.get("data", {})
    comments = response_data_section.get("comments")

    return TikTokCommentsResponse(comments=comments, data=response_data_section, error=error_data)


def _extract_response_json_or_raise_error(response: rq.Response | None) -> Mapping[str, Any]:
    if response is None:
        raise ValueError("Response is None")

    try:
        return response.json()
    except rq.exceptions.JSONDecodeError:
        logging.info(
            "Error parsing JSON response:\n%s\n%s\n%s\n%s",
            response.url,
            response.status_code,
            "\n".join([f"{k}: {v}" for k, v in response.headers.items()]),
            response.text,
        )
        raise


def update_crawl_from_api_response(
    crawl: Crawl, api_response: TikTokVideoResponse, num_videos_requested: int = 100
):
    crawl.cursor = api_response.data["cursor"]
    crawl.has_more = api_response.data["has_more"]

    if "search_id" in api_response.data and api_response.data["search_id"] != crawl.search_id:
        if crawl.search_id is not None:
            logging.log(
                logging.ERROR,
                f"search_id changed! Was {crawl.search_id} now {api_response.data['search_id']}",
            )
        crawl.search_id = api_response.data["search_id"]

    crawl.updated_at = pendulum.now("UTC")

    # Update the number of videos that were possibly deleted
    if crawl.extra_data is None:
        current_deleted_count = 0
    else:
        current_deleted_count = crawl.extra_data.get("possibly_deleted", 0)

    n_videos = len(api_response.videos)

    crawl.extra_data = {
        "possibly_deleted": (num_videos_requested - n_videos) + current_deleted_count
    }


@attrs.define
class TikTokApiClient:
    """Provides interface for interacting with TikTok research API. Handles requests to API to
    get/refresh access token and requests required to fetch all API query responses (especially if
    results are paginated).

    Can be used to fetch only API results, and additionally store those results in a database (if
    database engine provided in config).

    This client caches responses for userinfo and comments for a video ID. The cache is unbounded,
    so if you need to reduce memory usage, or you want to make sure the API is queried, you can call
    .clear_cache()
    """

    _request_client: TikTokApiRequestClient = attrs.field(kw_only=True)
    _config: ApiClientConfig = attrs.field(
        validator=[attrs.validators.instance_of(ApiClientConfig), field_is_not_empty]
    )
    # Rudimentary cache of username -> user_info
    _user_info_cache: Mapping[str, TikTokUserInfoResponse] = attrs.field(
        default=None, kw_only=True, converter=attrs.converters.default_if_none(factory=dict)
    )
    # Rudimentary cache of video_id -> comments
    _comments_cache: Mapping[str, TikTokVideoResponse] = attrs.field(
        default=None, kw_only=True, converter=attrs.converters.default_if_none(factory=dict)
    )

    @classmethod
    def from_config(cls, config: ApiClientConfig, request_client=None, **kwargs) -> TikTokApiClient:
        return cls(
            **kwargs,
            config=config,
            request_client=TikTokApiRequestClient.from_credentials_file(
                credentials_file=config.api_credentials_file,
                raw_responses_output_dir=config.raw_responses_output_dir,
                max_api_rate_limit_retries=config.max_api_rate_limit_retries,
                max_api_requests=config.max_api_requests,
                api_rate_limit_wait_strategy=config.api_rate_limit_wait_strategy,
            ),
        )

    @property
    def num_api_requests_sent(self):
        return self._request_client.num_api_requests_sent

    def reset_num_requests(self):
        self._request_client.reset_num_requests()

    @property
    def expected_remaining_api_request_quota(self):
        return DAILY_API_REQUEST_QUOTA - self.num_api_requests_sent

    @property
    def max_api_requests_reached(self):
        return self._request_client.max_api_requests_reached()

    def clear_cache(self):
        """Clears cache on both fetch_video_comments, and fetch_user_info"""
        self._user_info_cache.clear()
        self._comments_cache.clear()

    def api_results_iter(self, query_config: VideoQueryConfig) -> TikTokApiClientFetchResult:
        """Fetches all results from API (ie requests until API indicates query results have been
        fully delivered (has_more == False)). Yielding each API response individually.
        """
        crawl = Crawl.from_query(
            query=query_config.query,
            crawl_tags=query_config.crawl_tags,
            # Set has_more to True since we have not yet made an API request
            has_more=True,
        )
        logging.debug("Crawl: %s", crawl)

        logging.info("Beginning API results fetch.")
        while crawl.has_more:
            request = TikTokVideoRequest.from_config(
                config=query_config,
                cursor=crawl.cursor,
                search_id=crawl.search_id,
            )
            videos = None
            user_info = None
            comments = None
            try:
                api_response = self._request_client.fetch_videos(request)
                videos = api_response.videos

                if api_response.data:
                    logging.debug(
                        "api_response.data: cursor: %s, has_more: %s, search_id: %s",
                        api_response.data.get("cursor"),
                        api_response.data.get("has_more"),
                        api_response.data.get("search_id"),
                    )
                    logging.debug("API response error section: %s", api_response.error)
                    logging.debug("API response videos results:\n%s", api_response.videos)

                update_crawl_from_api_response(
                    crawl=crawl,
                    api_response=api_response,
                    num_videos_requested=query_config.max_count,
                )

                if query_config.fetch_user_info:
                    user_info_responses = self._fetch_user_info_for_videos_in_response(api_response)
                    if user_info_responses:
                        user_info = [response.user_info for response in user_info_responses]

                if query_config.fetch_comments:
                    comments_responses = self._fetch_comments_for_videos_in_response(api_response)
                    if comments_responses:
                        # Flatten list of comments from list of responses
                        comments = [
                            comment
                            for response in comments_responses
                            for comment in response.comments
                        ]
            except MaxApiRequestsReachedError as e:
                logging.info("Stopping api_results_iter due to %r", e)
                break
            except (ApiServerError, InvalidSearchIdError, InvalidCountOrCursorError) as e:
                if self._config.raise_error_on_persistent_api_server_error:
                    raise e from None
                break

            finally:
                # TODO(macpd): test partial result yielding
                # Yield results, including partial results that may exist due to exception
                if any([videos, user_info, comments]):
                    yield TikTokApiClientFetchResult(
                        videos=videos,
                        user_info=user_info,
                        comments=comments,
                        crawl=crawl,
                    )

        logging.info(
            "Crawl completed (or reached configured max_api_requests: %s). Num api requests: %s. "
            "Expected remaining API request quota: %s",
            self._config.max_api_requests,
            self.num_api_requests_sent,
            self.expected_remaining_api_request_quota,
        )

    def _fetch_user_info_for_videos_in_response(
        self, api_video_response: TikTokVideoResponse
    ) -> Sequence[TikTokUserInfoResponse]:
        user_info_responses = []
        for username in {video.get("username") for video in api_video_response.videos}:
            user_info_response = self.fetch_user_info(username)
            if response_is_ok(user_info_response):
                user_info_responses.append(user_info_response)
            else:
                logging.warning("Error fetching user info for %s: %s", username, user_info_response)
        return user_info_responses

    def fetch_user_info(self, username: str) -> TikTokUserInfoResponse:
        if username not in self._user_info_cache:
            try:
                self._user_info_cache[username] = self._request_client.fetch_user_info(
                    TikTokUserInfoRequest(username)
                )
            except (InvalidUsernameError, RefusedUsernameError) as e:
                logging.info("username %s not found. %s", username, e)
                self._user_info_cache[username] = TikTokUserInfoResponse(
                    username=username, error=e.error_json, user_info=None, data=None
                )

        return self._user_info_cache[username]

    def _fetch_comments_for_videos_in_response(
        self, api_video_response: TikTokVideoResponse
    ) -> Sequence[TikTokCommentsResponse]:
        comment_responses = []
        for video in api_video_response.videos:
            comment_responses.extend(self.fetch_video_comments(video.get("id")))

        return comment_responses

    def fetch_video_comments(self, video_id: int | str) -> Sequence[TikTokCommentsResponse]:
        if video_id not in self._comments_cache:
            self._comments_cache[video_id] = self._fetch_video_comments(video_id)
        return self._comments_cache[video_id]

    def _fetch_video_comments(self, video_id: int | str) -> Sequence[TikTokCommentsResponse]:
        comment_responses = []
        has_more = True
        cursor = None
        while has_more:
            response = self._request_client.fetch_comments(
                TikTokCommentsRequest(video_id=video_id, cursor=cursor)
            )
            if not response_is_ok(response):
                logging.warning("Error fetching comments for video id %s: %s", video_id, response)
                break

            # Only add response if video has comments
            if response.comments:
                comment_responses.append(response)

            has_more = response.data.get("has_more", False)
            cursor = response.data.get("cursor")

            if cursor > MAX_COMMENTS_CURSOR:
                logging.debug(
                    "Stopping comments fetch for video ID %s because cursor %s excceds "
                    "maximum API allows (%s)",
                    video_id,
                    cursor,
                    MAX_COMMENTS_CURSOR,
                )
                has_more = False

        return comment_responses

    def fetch_all(
        self, query_config: VideoQueryConfig, *args, store_results_after_each_response: bool = False
    ) -> TikTokApiClientFetchResult:
        """Fetches all results from API (ie sends requests until API indicates query results have
        been fully delivered (has_more == False))

        Args:
            store_results_after_each_response: bool, if true used database engine from config to
            store api results in database after each response is received (and before requesting
            next page of results).
        """
        if args:
            raise ValueError("This function does not allow any positional arguments")
        video_data = []
        user_info = []
        comments = []
        crawl = None
        for api_response in self.api_results_iter(query_config):
            video_data.extend(api_response.videos)
            if api_response.user_info:
                user_info.extend(api_response.user_info)
            if api_response.comments:
                comments.extend(api_response.comments)
            if store_results_after_each_response and api_response.videos:
                self.store_fetch_result(fetch_result=api_response)
            crawl = api_response.crawl

        logging.debug("fetch_all video results:\n%s", video_data)
        return TikTokApiClientFetchResult(
            videos=video_data,
            user_info=user_info or None,
            comments=comments or None,
            crawl=crawl,
        )

    def store_fetch_result(
        self,
        fetch_result: TikTokApiClientFetchResult,
    ):
        """Stores API results to database."""
        logging.debug("Putting crawl to database: %s", fetch_result.crawl)
        fetch_result.crawl.upload_self_to_db(self._config.engine)
        logging.debug("Upserting videos")
        upsert_videos(
            video_data=fetch_result.videos,
            crawl_id=fetch_result.crawl.id,
            crawl_tags=fetch_result.crawl.crawl_tags,
            engine=self._config.engine,
        )
        if fetch_result.user_info:
            upsert_user_info(user_info_sequence=fetch_result.user_info, engine=self._config.engine)
        if fetch_result.comments:
            upsert_comments(comments=fetch_result.comments, engine=self._config.engine)

    def fetch_and_store_all(self, query_config: VideoQueryConfig) -> TikTokApiClientFetchResult:
        return self.fetch_all(query_config=query_config, store_results_after_each_response=True)
