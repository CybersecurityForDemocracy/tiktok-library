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

from .query import Query, QueryJSONEncoder

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
    request_data: Mapping[str, Any]
    videos: Sequence[Any]


@attrs.define
class AcquitionConfig:
    query: Query
    start_date: datetime
    final_date: datetime
    engine: Engine
    api_credentials_file: Path
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
            start_date=config.start_date.strftime("%Y%m%d"),
            end_date=config.final_date.strftime("%Y%m%d"),
            **kwargs,
        )

    def as_json(self):
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
        return json.dumps(request_obj, cls=QueryJSONEncoder, indent=1)


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

    @classmethod
    def from_credentials_file(
        cls, credentials_file: Path, *args, **kwargs
    ) -> TikTokApiRequestClient:
        with credentials_file.open("r") as f:
            dict_credentials = yaml.load(f, Loader=yaml.FullLoader)

        return cls(
            credentials=TiktokCredentials(**dict_credentials),
            *args,
            **kwargs,
        )

    def __attrs_post_init__(self):
        self._configure_session()

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
        logging.info(f"Access token response: {access_data}")

        token = access_data["access_token"]

        return token

    @_access_token_fetcher_session.default  # type: ignore - Attrs overload
    def _default_access_token_fetcher_session(self):
        return rq.Session()

    @_api_request_session.default  # type: ignore - Attrs overload
    def _make_session(self):
        return rq.Session()

    def _configure_session(self):
        """Gets access token for authorization, sets token in headers for all request, and registers
        a hook to refresh token when response indicates it has expired."""
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
        return self._parse_response(api_response)

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

        if self._raw_responses_output_dir is not None:
            self._store_response(req)

        if req.status_code == 200:
            return req

        if req.status_code == 429:
            raise ApiRateLimitError(repr(req))

        if req.status_code == 400:
            raise InvalidRequestError(f"{req!r} {req.text}")

        logging.log(
            logging.ERROR,
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
            req_data = response.json().get("data", {})
        except rq.exceptions.JSONDecodeError as e:
            logging.info(
                "Error parsing JSON response:\n%s\n%s\n%s",
                response.status_code,
                "\n".join([f"{k}: {v}" for k, v in response.headers.items()]),
                response.text,
            )
            raise e

        videos = req_data.get("videos", [])

        return TikTokResponse(request_data=req_data, videos=videos)
