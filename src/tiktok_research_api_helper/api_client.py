from __future__ import annotations

import enum
import json
import logging
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
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
from tiktok_research_api_helper.models import Crawl, upsert_videos
from tiktok_research_api_helper.query import Query, QueryJSONEncoder

ALL_VIDEO_DATA_URL = "https://open.tiktokapis.com/v2/research/video/query/?fields=id,video_description,create_time,region_code,share_count,view_count,like_count,comment_count,music_id,hashtag_names,username,effect_ids,voice_to_text,playlist_id"

SEARCH_ID_INVALID_ERROR_MESSAGE_REGEX = re.compile(r"Search Id \d+ is invalid or expired")

INVALID_SEARCH_ID_ERROR_RETRY_WAIT = 5
INVALID_SEARCH_ID_ERROR_MAX_NUM_RETRIES = 5


class ApiRateLimitError(Exception):
    pass


class InvalidRequestError(Exception):
    pass


class InvalidSearchIdError(InvalidRequestError):
    pass


def field_is_not_empty(instance, attribute, value):
    if not value:
        raise ValueError(f"{instance.__class__.__name__}: {attribute.name} cannot be empty")


class ApiRateLimitWaitStrategy(enum.StrEnum):
    WAIT_FOUR_HOURS = enum.auto()
    WAIT_NEXT_UTC_MIDNIGHT = enum.auto()


@attrs.define
class TiktokCredentials:
    client_id: str = attrs.field(
        validator=[attrs.validators.instance_of((str, int)), field_is_not_empty]
    )
    client_secret: str = attrs.field(
        validator=[attrs.validators.instance_of(str), field_is_not_empty]
    )
    client_key: str = attrs.field(validator=[attrs.validators.instance_of(str), field_is_not_empty])


@attrs.define
class TikTokVideoResponse:
    data: Mapping[str, Any]
    videos: Sequence[Any]
    error: Mapping[str, Any]


@attrs.define
class TikTokApiClientFetchResult:
    videos: Sequence[Any]
    crawl: Crawl


@attrs.define
class ApiClientConfig:
    query: Query
    start_date: datetime
    end_date: datetime
    engine: Engine
    api_credentials_file: Path
    max_count: int = 100
    stop_after_one_request: bool = False
    crawl_tags: list[str] | None = None
    raw_responses_output_dir: Path | None = None
    api_rate_limit_wait_strategy: ApiRateLimitWaitStrategy = attrs.field(
        default=ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS,
        validator=attrs.validators.instance_of(ApiRateLimitWaitStrategy),  # type: ignore - Attrs overload
    )


@attrs.define
class TiktokVideoRequest:
    """
    A TiktokVideoRequest.

    The start date is inclusive but the end date is NOT.
    """

    query: Query
    start_date: str
    end_date: str  # The end date is NOT inclusive!
    max_count: int = 100
    is_random: bool = False

    cursor: int | None = None
    search_id: str | None = None

    @classmethod
    def from_config(cls, config: ApiClientConfig, **kwargs) -> TiktokVideoRequest:
        return cls(
            query=config.query,
            max_count=config.max_count,
            start_date=utils.date_to_tiktok_str_format(config.start_date),
            end_date=utils.date_to_tiktok_str_format(config.end_date),
            **kwargs,
        )

    def as_json(self, indent=None):
        request_obj = {
            "query": self.query,
            "max_count": self.max_count,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "is_random": self.is_random,
        }
        if self.search_id is not None:
            request_obj["search_id"] = self.search_id

        if self.cursor is not None:
            request_obj["cursor"] = self.cursor
        return json.dumps(request_obj, cls=QueryJSONEncoder, indent=indent)


def is_json_decode_error(exception):
    return isinstance(exception, rq.exceptions.JSONDecodeError | json.JSONDecodeError)


def retry_json_decoding_error_once(
    retry_state,
):
    exception = retry_state.outcome.exception()

    # Retry once if JSON decoding response fails
    if is_json_decode_error(exception):
        return retry_state.attempt_number <= 1

    return None


def retry_invalid_search_id_error(
    retry_state,
):
    exception = retry_state.outcome.exception()

    # Workaround API bug where valid search ID (ie the one the API just returned) is rejected as
    # invalid.
    if isinstance(exception, InvalidSearchIdError):
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


def json_decoding_error_retry_immediately(
    retry_state,
):
    exception = retry_state.outcome.exception()
    # If JSON decoding fails retry immediately
    if is_json_decode_error(exception):
        return 0

    logging.warning("Unknown exception in wait callback: %r", exception)
    return 0


def search_id_invalid_error_wait(
    retry_state,
):
    exception = retry_state.outcome.exception()
    # Wait in case API needs a few seconds to consider search ID valid.
    if isinstance(exception, InvalidSearchIdError):
        return INVALID_SEARCH_ID_ERROR_RETRY_WAIT

    logging.warning("Unknown exception in wait callback: %r", exception)
    return 0


def get_api_rate_limit_wait_strategy(
    api_rate_limit_wait_strategy: ApiRateLimitWaitStrategy,
):
    if api_rate_limit_wait_strategy == ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS:
        return api_rate_limi_wait_four_hours
    if api_rate_limit_wait_strategy == ApiRateLimitWaitStrategy.WAIT_NEXT_UTC_MIDNIGHT:
        return api_rate_limi_wait_until_next_utc_midnight

    raise ValueError(f"Unknown wait strategy: {api_rate_limit_wait_strategy}")


def api_rate_limi_wait_until_next_utc_midnight(
    retry_state,
):
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

    logging.warning("Unknown exception in wait callback: %r", exception)
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

    logging.warning("Unknown exception in wait callback: %r", exception)
    return 0


@attrs.define
class TikTokApiRequestClient:
    """
    A class for making authenticated requests to the TikTok API and getting a parsed response.
    """

    _credentials: TiktokCredentials = attrs.field(
        validator=[attrs.validators.instance_of(TiktokCredentials), field_is_not_empty],
        # Attrs removes underscores from field names but the static type checker doesn't know that
        alias="credentials",
    )
    _access_token_fetcher_session: rq.Session = attrs.field()
    _api_request_session: rq.Session = attrs.field()
    _raw_responses_output_dir: Path | None = None
    _api_rate_limit_wait_strategy: ApiRateLimitWaitStrategy = attrs.field(
        default=ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS,
        validator=attrs.validators.instance_of(ApiRateLimitWaitStrategy),  # type: ignore - Attrs overload
    )
    _num_api_requests_sent: int = 0

    @classmethod
    def from_credentials_file(
        cls, credentials_file: Path, *args, **kwargs
    ) -> TikTokApiRequestClient:
        with credentials_file.open("r") as f:
            dict_credentials = yaml.load(f, Loader=yaml.FullLoader)

        return cls(
            *args,
            **kwargs,
            credentials=TiktokCredentials(**dict_credentials),
        )

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

    @_access_token_fetcher_session.default  # type: ignore - Attrs overload
    def _default_access_token_fetcher_session(self):
        return rq.Session()

    @_api_request_session.default  # type: ignore - Attrs overload
    def _make_session(self):
        return rq.Session()

    @property
    def num_api_requests_sent(self):
        return self._num_api_requests_sent

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

    def _fetch_retryer(self, max_api_rate_limit_retries=None):
        if max_api_rate_limit_retries is not None:
            stop_strategy = tenacity.stop_after_attempt(max_api_rate_limit_retries)
        else:
            stop_strategy = tenacity.stop_never

        return tenacity.Retrying(
            retry=tenacity.retry_any(
                retry_json_decoding_error_once,
                retry_invalid_search_id_error,
                retry_api_rate_limit_error_indefintely,
            ),
            wait=tenacity.wait_combine(
                json_decoding_error_retry_immediately,
                search_id_invalid_error_wait,
                get_api_rate_limit_wait_strategy(
                    api_rate_limit_wait_strategy=self._api_rate_limit_wait_strategy
                ),
            ),
            stop=stop_strategy,
            reraise=True,
        )

    def fetch(self, request: TiktokVideoRequest, max_api_rate_limit_retries=None) -> TikTokVideoResponse:
        return self._fetch_retryer(max_api_rate_limit_retries=max_api_rate_limit_retries)(
            self._fetch, request
        )

    def _fetch(self, request: TiktokVideoRequest) -> TikTokVideoResponse:
        api_response = self._post(request)
        self._num_api_requests_sent += 1
        return _parse_video_response(api_response)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(10),
        wait=tenacity.wait_exponential(
            multiplier=1, min=3, max=timedelta(minutes=5).total_seconds()
        ),
        retry=tenacity.retry_if_exception_type(rq.RequestException),
        reraise=True,
    )
    def _post(self, request: TiktokVideoRequest) -> rq.Response | None:
        data = request.as_json()
        logging.log(logging.INFO, f"Sending request with data: {data}")

        response = self._api_request_session.post(url=ALL_VIDEO_DATA_URL, data=data)
        logging.debug("%s\n%s", response, response.text)

        if self._raw_responses_output_dir is not None:
            self._store_response(response)

        if response.status_code == 200:
            return response

        if response.status_code == 429:
            raise ApiRateLimitError(repr(response))

        if response.status_code == 400:
            try:
                response_json = response.json()
                if SEARCH_ID_INVALID_ERROR_MESSAGE_REGEX.match(
                    response_json.get("error", {}).get("message", "")
                ):
                    raise InvalidSearchIdError(f"{response!r} {response.text}")
            except json.JSONDecodeError:
                logging.debug("Unable to JSON decode response data:\n%s", response.text)

            raise InvalidRequestError(f"{response!r} {response.text}")

        if response.status_code == 500:
            logging.info("API responded 500. This happens occasionally")
        else:
            logging.warning(
                f"Request failed, status code {response.status_code} - text {response.text} - data "
                "{data}",
            )
        response.raise_for_status()
        # In case raise_for_status does not raise an exception we return None
        return None

def _parse_video_response(response: rq.Response | None) -> TikTokVideoResponse:
    response_json = _extract_response_json(response)
    error_data = response_json.get("error")
    response_data_section = response_json.get("data", {})
    videos = response_data_section.get("videos", [])

    return TikTokVideoResponse(data=response_data_section, videos=videos, error=error_data)


def _extract_response_json(response: rq.Response | None) -> Mapping[str, Any]:
    if response is None:
        raise ValueError("Response is None")

    try:
        return response.json()
    except rq.exceptions.JSONDecodeError:
        logging.info(
            "Error parsing JSON response:\n%s\n%s\n%s",
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
    """

    _request_client: TikTokApiRequestClient = attrs.field()
    _config: ApiClientConfig = attrs.field(
        validator=[attrs.validators.instance_of(ApiClientConfig), field_is_not_empty]
    )

    @classmethod
    def from_config(cls, config: ApiClientConfig, *args, **kwargs) -> TikTokApiClient:
        return cls(
            *args,
            **kwargs,
            config=config,
            request_client=TikTokApiRequestClient.from_credentials_file(
                credentials_file=config.api_credentials_file,
                raw_responses_output_dir=config.raw_responses_output_dir,
            ),
        )

    @property
    def num_api_requests_sent(self):
        return self._request_client.num_api_requests_sent

    @property
    def expected_remaining_api_request_quota(self):
        return 1000 - self.num_api_requests_sent

    def api_results_iter(self) -> TikTokApiClientFetchResult:
        """Fetches all results from API (ie requests until API indicates query results have been
        fully delivered (has_more == False)). Yielding each API response individually.
        """
        crawl = Crawl.from_query(
            query=self._config.query,
            crawl_tags=self._config.crawl_tags,
            # Set has_more to True since we have not yet made an API request
            has_more=True,
        )
        logging.debug("Crawl: %s", crawl)

        logging.info("Beginning API results fetch.")
        while crawl.has_more:
            request = TiktokVideoRequest.from_config(
                config=self._config,
                cursor=crawl.cursor,
                search_id=crawl.search_id,
            )
            api_response = self._request_client.fetch(request)

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
                num_videos_requested=self._config.max_count,
            )

            yield TikTokApiClientFetchResult(videos=api_response.videos, crawl=crawl)

            if not api_response.videos and crawl.has_more:
                logging.log(
                    logging.ERROR,
                    "No videos in response but there's still data to Crawl - Query: "
                    f"{self._config.query} \n api_response.data: {api_response.data}",
                )
            if self._config.stop_after_one_request:
                logging.info("Stopping after one request")
                break

        logging.info(
            "Crawl completed. Num api requests: %s. Expected remaining API request quota: %s",
            self.num_api_requests_sent,
            self.expected_remaining_api_request_quota,
        )

    def fetch_all(
        self, *, store_results_after_each_response: bool = False
    ) -> TikTokApiClientFetchResult:
        """Fetches all results from API (ie sends requests until API indicates query results have
        been fully delivered (has_more == False))

        Args:
            store_results_after_each_response: bool, if true used database engine from config to
            store api results in database after each response is received (and before requesting
            next page of results).
        """
        video_data = []
        for api_response in self.api_results_iter():
            video_data.extend(api_response.videos)
            if store_results_after_each_response and video_data:
                self.store_fetch_result(api_response)

        logging.debug("fetch_all video results:\n%s", video_data)
        return TikTokApiClientFetchResult(videos=video_data, crawl=api_response.crawl)

    def store_fetch_result(self, fetch_result: TikTokApiClientFetchResult):
        """Stores API results to database."""
        logging.debug("Putting crawl to database: %s", fetch_result.crawl)
        fetch_result.crawl.upload_self_to_db(self._config.engine)
        logging.debug("Upserting videos")
        upsert_videos(
            video_data=fetch_result.videos,
            crawl_id=fetch_result.crawl.id,
            crawl_tags=self._config.crawl_tags,
            engine=self._config.engine,
        )

    def fetch_and_store_all(self) -> TikTokApiClientFetchResult:
        return self.fetch_all(store_results_after_each_response=True)
