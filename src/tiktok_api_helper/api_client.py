from __future__ import annotations

import enum
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import attrs
import certifi
import pendulum
import requests as rq
import tenacity
import yaml
from sqlalchemy import Engine

from tiktok_api_helper.query import Query, QueryJSONEncoder
from tiktok_api_helper.sql import (
    Crawl,
    upsert_videos
)

ALL_VIDEO_DATA_URL = "https://open.tiktokapis.com/v2/research/video/query/?fields=id,video_description,create_time,region_code,share_count,view_count,like_count,comment_count,music_id,hashtag_names,username,effect_ids,voice_to_text,playlist_id"


class ApiRateLimitError(Exception):
    pass


class InvalidRequestError(Exception):
    pass


def field_is_not_empty(instance, attribute, value):
    if not value:
        raise ValueError(
            f"{instance.__class__.__name__}: {attribute.name} cannot be empty"
        )


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
    client_key: str = attrs.field(
        validator=[attrs.validators.instance_of(str), field_is_not_empty]
    )


@attrs.define
class TikTokResponse:
    data: Mapping[str, Any]
    videos: Sequence[Any]
    error: Mapping[str, Any]

@attrs.define
class TikTokApiClientFetchResult:
    videos: Sequence[Any]
    crawl: Crawl


@attrs.define
class AcquitionConfig:
    query: Query
    start_date: datetime
    final_date: datetime
    engine: Engine
    api_credentials_file: Path
    max_count: int = 100
    stop_after_one_request: bool = False
    crawl_tags: Optional[list[str]] = None
    raw_responses_output_dir: Optional[Path] = None
    api_rate_limit_wait_strategy: ApiRateLimitWaitStrategy = attrs.field(
        default=ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS,
        validator=attrs.validators.instance_of(ApiRateLimitWaitStrategy),  # type: ignore - Attrs overload
    )


@attrs.define
class TiktokRequest:
    """
    A TiktokRequest.

    The start date is inclusive but the end date is NOT.
    """

    query: Query
    start_date: str
    end_date: str  # The end date is NOT inclusive!
    max_count: int = 100
    is_random: bool = False

    cursor: Optional[int] = None
    search_id: Optional[str] = None

    @classmethod
    def from_config(cls, config: AcquitionConfig, **kwargs) -> TiktokRequest:
        return cls(
            query=config.query,
            max_count=config.max_count,
            start_date=config.start_date.strftime("%Y%m%d"),
            end_date=config.final_date.strftime("%Y%m%d"),
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


def retry_once_if_json_decoding_error_or_retry_indefintely_if_api_rate_limit_error(
    retry_state,
):
    exception = retry_state.outcome.exception()

    # No exception, call succeeded
    if exception is None:
        return False

    # Retry once if JSON decoding response fails
    if isinstance(exception, (rq.exceptions.JSONDecodeError, json.JSONDecodeError)):
        return retry_state.attempt_number <= 1

    # TODO(macpd): remove or improve this
    # Retry twice on invalid request error
    if isinstance(exception, InvalidRequestError):
        return retry_state.attempt_number <= 1

    # Retry API rate lmiit errors indefinitely.
    if isinstance(exception, ApiRateLimitError):
        return True

    logging.warning("Retry call back received unexpected retry state: %r", retry_state)
    return False


def json_decoding_error_retry_immediately_or_api_rate_limi_wait_until_next_utc_midnight(
    retry_state,
):
    exception = retry_state.outcome.exception()
    # If JSON decoding fails retry immediately
    if isinstance(exception, (rq.exceptions.JSONDecodeError, json.JSONDecodeError)):
        return 0

    if isinstance(exception, ApiRateLimitError):
        next_utc_midnight = pendulum.tomorrow("UTC")
        logging.warning(
            "Response indicates rate limit exceeded: %r\nSleeping until next UTC midnight: %s (local time %s). Will resume in approx %s",
            exception,
            next_utc_midnight,
            next_utc_midnight.in_tz("local"),
            next_utc_midnight.diff_for_humans(pendulum.now("local"), absolute=True),
        )
        return (next_utc_midnight - pendulum.now()).seconds

    logging.warning("Unknown exception in wait callback: %r", exception)
    return 0


def json_decoding_error_retry_immediately_or_api_rate_limi_wait_four_hours(
    retry_state,
):
    exception = retry_state.outcome.exception()
    # If JSON decoding fails retry immediately
    if isinstance(exception, (rq.exceptions.JSONDecodeError, json.JSONDecodeError)):
        return 0

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
        alias="credentials",  # Attrs removes underscores from field names but the static type checker doesn't know that
    )
    _access_token_fetcher_session: rq.Session = attrs.field()
    _api_request_session: rq.Session = attrs.field()
    _raw_responses_output_dir: Optional[Path] = None
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

        token = access_data["access_token"]

        return token

    @_access_token_fetcher_session.default  # type: ignore - Attrs overload
    def _default_access_token_fetcher_session(self):
        return rq.Session()

    @_api_request_session.default  # type: ignore - Attrs overload
    def _make_session(self):
        return rq.Session()

    def _configure_request_sessions(self):
        """Gets access token for authorization, sets token in headers for all requests, and registers
        a hook to refresh token when response indicates it has expired. Configures access token
        fetcher and api fetcher sessions to verify certs using certifi."""
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
        *unused_args: Optional[Sequence],
        **unused_kwargs: Optional[Mapping[Any, Any]],
    ) -> rq.Response | None:
        # Adapted from https://stackoverflow.com/questions/37094419/python-requests-retry-request-after-re-authentication
        if r.status_code == 401:
            logging.info("Fetching new token as the previous token expired")

            token = self._get_client_access_token()
            self._api_request_session.headers.update(
                {"Authorization": f"Bearer {token}"}
            )

            r.request.headers["Authorization"] = self._api_request_session.headers[
                "Authorization"
            ]

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

        if (
            self._api_rate_limit_wait_strategy
            == ApiRateLimitWaitStrategy.WAIT_FOUR_HOURS
        ):
            wait_strategy = (
                json_decoding_error_retry_immediately_or_api_rate_limi_wait_four_hours
            )
        elif (
            self._api_rate_limit_wait_strategy
            == ApiRateLimitWaitStrategy.WAIT_NEXT_UTC_MIDNIGHT
        ):
            wait_strategy = json_decoding_error_retry_immediately_or_api_rate_limi_wait_until_next_utc_midnight
        else:
            raise ValueError(
                f"Unknown wait strategy: {self._api_rate_limit_wait_strategy}"
            )

        return tenacity.Retrying(
            retry=retry_once_if_json_decoding_error_or_retry_indefintely_if_api_rate_limit_error,
            wait=wait_strategy,
            stop=stop_strategy,
            reraise=True,
        )

    def fetch(
        self, request: TiktokRequest, max_api_rate_limit_retries=None
    ) -> TikTokResponse:
        return self._fetch_retryer(
            max_api_rate_limit_retries=max_api_rate_limit_retries
        )(self._fetch, request)

    def _fetch(self, request: TiktokRequest) -> TikTokResponse:
        api_response = self._post(request)
        self._num_api_requests_sent += 1
        return self._parse_response(api_response)

    @property
    def num_api_requests_sent(self):
        return self._num_api_requests_sent

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(10),
        wait=tenacity.wait_exponential(
            multiplier=1, min=3, max=timedelta(minutes=5).total_seconds()
        ),
        retry=tenacity.retry_if_exception_type(rq.RequestException),
        reraise=True,
    )
    def _post(self, request: TiktokRequest) -> rq.Response | None:
        data = request.as_json()
        logging.log(logging.INFO, f"Sending request with data: {data}")

        req = self._api_request_session.post(url=ALL_VIDEO_DATA_URL, data=data)
        logging.debug(req)

        if self._raw_responses_output_dir is not None:
            self._store_response(req)

        if req.status_code == 200:
            return req

        if req.status_code == 429:
            raise ApiRateLimitError(repr(req))

        if req.status_code == 400:
            raise InvalidRequestError(f"{req!r} {req.text}")

        if req.status_code == 500:
            logging.info("API responded 500. This happens occasionally")
        else:
            logging.warning(
                f"Request failed, status code {req.status_code} - text {req.text} - data {data}",
            )
        req.raise_for_status()
        # In case raise_for_status does not raise an exception we return None
        return None

    @staticmethod
    def _parse_response(response: Optional[rq.Response]) -> TikTokResponse:
        if response is None:
            raise ValueError("Response is None")

        try:
            response_json = response.json()
            response_data_section = response_json.get("data", {})
            error_data = response_json.get("error")
        except rq.exceptions.JSONDecodeError as e:
            logging.info(
                "Error parsing JSON response:\n%s\n%s\n%s",
                response.status_code,
                "\n".join([f"{k}: {v}" for k, v in response.headers.items()]),
                response.text,
            )
            raise e

        videos = response_data_section.get("videos", [])

        return TikTokResponse(data=response_data_section, videos=videos, error=error_data)

def update_crawl_from_api_response(
        crawl: Crawl, api_response: TikTokResponse, num_videos_requested: int = 100
):
    crawl.cursor = api_response.data["cursor"]
    crawl.has_more = api_response.data["has_more"]

    if api_response.data["search_id"] != crawl.search_id:
        if crawl.search_id is not None:
            logging.log(
                logging.ERROR,
                f"search_id changed! Was {crawl.search_id} now {api_response.data['search_id']}",
            )
        crawl.search_id = api_response.data["search_id"]

    crawl.updated_at = datetime.now()

    # Update the number of videos that were possibly deleted
    if crawl.extra_data is None:
        current_deleted_count = 0
    else:
        current_deleted_count = crawl.extra_data.get("possibly_deleted", 0)

    n_videos = len(api_response.videos)

    crawl.extra_data = {"possibly_deleted": (num_videos_requested - n_videos) + current_deleted_count}

@attrs.define
class TikTokApiClient:
    """Provides interface for interacting with TikTok research API. Handles requests to API to
    get/refresh access token and requests required to fetch all API query responses (especially if
    results are paginated).

    Can be used to fetch only API results, and additionally store those results in a database (if
    database engine provided in config).
    """
    _request_client: TikTokApiRequestClient = attrs.field(
        validator=[attrs.validators.instance_of(TikTokApiRequestClient), field_is_not_empty])
    _config: AcquitionConfig = attrs.field(
        validator=[attrs.validators.instance_of(AcquitionConfig), field_is_not_empty])

    @classmethod
    def from_config(
        cls, config: AcquitionConfig, *args, **kwargs
    ) -> TikTokApiClient:
        return cls(
            *args,
            **kwargs,
            config=config,
            request_client=TikTokApiRequestClient.from_credentials_file(
                credentials_file=config.api_credentials_file,
                raw_responses_output_dir=config.raw_responses_output_dir)
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
        crawl = Crawl.from_query(query=self._config.query, crawl_tags=self._config.crawl_tags,
                                 # Set has_more to True since we have not yet made an API request
                                 has_more=True)
        logging.debug('Crawl: %s', crawl)

        logging.info('Beginning API results fetch.')
        while crawl.has_more:
            request = TiktokRequest.from_config(
                config=self._config,
                cursor=crawl.cursor,
                search_id=crawl.search_id,
            )
            api_response = self._request_client.fetch(request)

            if api_response.data:
                logging.debug("api_response.data: cursor: %s, has_more: %s, search_id: %s",
                              api_response.data.get('cursor'), api_response.data.get('has_more'),
                              api_response.data.get('search_id'))
                logging.debug('API response error section: \n%s', api_response.error)
                logging.debug('API response videos results:\n%s', api_response.videos)

            update_crawl_from_api_response(crawl=crawl, api_response=api_response,
                                           num_videos_requested=self._config.max_count)

            yield TikTokApiClientFetchResult(videos=api_response.videos, crawl=crawl)

            if not api_response.videos and crawl.has_more:
                logging.log(
                    logging.ERROR,
                    f"No videos in response but there's still data to Crawl - Query: {self._config.query} \n api_response.data: {api_response.data}",
                )
            if self._config.stop_after_one_request:
                logging.info("Stopping after one request")
                break

        logging.info(
                "Crawl completed. Num api requests: %s. Expected remaining API request quota: %s", self.num_api_requests_sent, self.expected_remaining_api_request_quota)


    def fetch_all(self, store_results_after_each_response: bool = False) -> TikTokApiClientFetchResult:
        """Fetches all results from API (ie requests until API indicates query results have been
        fully delivered (has_more == False))

        Args:
            store_results_after_each_response: bool, if true used database engine from config to
            store api results in database after each response is received (and before requesting
            next page of results).
        """
        video_data = []
        for api_response in self.api_results_iter():
            video_data.extend(api_response.videos)
            if store_results_after_each_response:
                self.store_fetch_result(api_response)

        logging.debug('fetch_all video results:\n%s', video_data)
        return TikTokApiClientFetchResult(videos=video_data, crawl=api_response.crawl)

    def store_fetch_result(self, fetch_result: TikTokApiClientFetchResult):
        """Stores API results to database."""
        logging.debug('Putting crawl to database: %s', fetch_result.crawl)
        fetch_result.crawl.upload_self_to_db(self._config.engine)
        logging.debug('Upserting videos')
        upsert_videos(video_data=fetch_result.videos,
                      crawl_id=fetch_result.crawl.id,
                      crawl_tags=self._config.crawl_tags,
                      engine=self._config.engine)

    def fetch_and_store_all(self) -> TikTokApiClientFetchResult:
        return self.fetch_all(store_results_after_each_response=True)

