from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Callable, Optional, Sequence, Union, Mapping, Any
from pathlib import Path
import json

import attrs
from attr import dataclass
import requests as rq
import yaml
import certifi
from sqlalchemy import Engine
import tenacity
import pause
import pendulum

# fmt: off
SUPPORTED_OPERATIONS = ["EQ", "IN", "GT", "GTE", "LT", "LTE"]
SUPPORTED_REGION_CODES = [ 'FR', 'TH', 'MM', 'BD', 'IT', 'NP', 'IQ', 'BR', 'US', 'KW', 'VN', 'AR', 'KZ', 'GB', 'UA', 'TR', 'ID', 'PK', 'NG', 'KH', 'PH', 'EG', 'QA', 'MY', 'ES', 'JO', 'MA', 'SA', 'TW', 'AF', 'EC', 'MX', 'BW', 'JP', 'LT', 'TN', 'RO', 'LY', 'IL', 'DZ', 'CG', 'GH', 'DE', 'BJ', 'SN', 'SK', 'BY', 'NL', 'LA', 'BE', 'DO', 'TZ', 'LK', 'NI', 'LB', 'IE', 'RS', 'HU', 'PT', 'GP', 'CM', 'HN', 'FI', 'GA', 'BN', 'SG', 'BO', 'GM', 'BG', 'SD', 'TT', 'OM', 'FO', 'MZ', 'ML', 'UG', 'RE', 'PY', 'GT', 'CI', 'SR', 'AO', 'AZ', 'LR', 'CD', 'HR', 'SV', 'MV', 'GY', 'BH', 'TG', 'SL', 'MK', 'KE', 'MT', 'MG', 'MR', 'PA', 'IS', 'LU', 'HT', 'TM', 'ZM', 'CR', 'NO', 'AL', 'ET', 'GW', 'AU', 'KR', 'UY', 'JM', 'DK', 'AE', 'MD', 'SE', 'MU', 'SO', 'CO', 'AT', 'GR', 'UZ', 'CL', 'GE', 'PL', 'CA', 'CZ', 'ZA', 'AI', 'VE', 'KG', 'PE', 'CH', 'LV', 'PR', 'NZ', 'TL', 'BT', 'MN', 'FJ', 'SZ', 'VU', 'BF', 'TJ', 'BA', 'AM', 'TD', 'SI', 'CY', 'MW', 'EE', 'XK', 'ME', 'KY', 'YE', 'LS', 'ZW', 'MC', 'GN', 'BS', 'PF', 'NA', 'VI', 'BB', 'BZ', 'CW', 'PS', 'FM', 'PG', 'BI', 'AD', 'TV', 'GL', 'KM', 'AW', 'TC', 'CV', 'MO', 'VC', 'NE', 'WS', 'MP', 'DJ', 'RW', 'AG', 'GI', 'GQ', 'AS', 'AX', 'TO', 'KN', 'LC', 'NC', 'LI', 'SS', 'IR', 'SY', 'IM', 'SC', 'VG', 'SB', 'DM', 'KI', 'UM', 'SX', 'GD', 'MH', 'BQ', 'YT', 'ST', 'CF', 'BM', 'SM', 'PW', 'GU', 'HK', 'IN', 'CK', 'AQ', 'WF', 'JE', 'MQ', 'CN', 'GF', 'MS', 'GG', 'TK', 'FK', 'PM', 'NU', 'MF', 'ER', 'NF', 'VA', 'IO', 'SH', 'BL', 'CU', 'NR', 'TP', 'BV', 'EH', 'PN', 'TF', 'RU']
# The duration of the video SHORT: <15s MID: 15 ~60s LONG: 1~5min EXTRA_LONG: >5min
SUPPORTED_VIDEO_LENGTHS = ["SHORT", "MID", "LONG", "EXTRA_LONG"]
# fmt: on

ALL_VIDEO_DATA_URL = "https://open.tiktokapis.com/v2/research/video/query/?fields=id,video_description,create_time,region_code,share_count,view_count,like_count,comment_count,music_id,hashtag_names,username,effect_ids,voice_to_text,playlist_id"


class ApiRateLimitError(Exception):
    pass


def sleep_until_next_utc_midnight() -> None:
    next_utc_midnight = pendulum.tomorrow("UTC")
    logging.warning(
        "Sleeping until next UTC midnight: %s (local time %s). Will resume in approx %s",
        next_utc_midnight,
        next_utc_midnight.in_tz("local"),
        next_utc_midnight.diff_for_humans(pendulum.now("local"), absolute=True),
    )
    pause.until(next_utc_midnight)


@dataclass
class TiktokCredentials:
    client_id: str
    client_secret: str
    client_key: str


@dataclass
class TikTokResponse:
    request_data: Mapping[str, Any]
    videos: Sequence[Any]


@attrs.define
class Operations:
    EQ = "EQ"
    IN = "IN"
    GT = "GT"
    GTE = "GTE"
    LT = "LT"
    LTE = "LTE"


INDENT = "\t"


@attrs.define
class AcquitionConfig:
    query: Query
    start_date: datetime
    final_date: datetime
    engine: Engine
    api_credentials_file: Path
    stop_after_one_request: bool = False
    source: Optional[list[str]] = None
    raw_responses_output_dir: Optional[Path] = None


def build_check_type(type) -> Callable[..., None]:

    def check_type(obj) -> None:
        assert isinstance(obj, type)

    return check_type


str_type_check = build_check_type(str)
int_type_check = build_check_type(int)


def check_can_convert_date(string: str) -> None:
    # We check by directly trying to convert; will raise an error otherwise
    datetime.strptime(string, "%Y%m%d")


@attrs.define
class VideoLength:
    SHORT = "SHORT"
    MID = "MID"
    LONG = "LONG"
    EXTRA_LONG = "EXTRA_LONG"


@attrs.define
class _Field:
    name: str
    validator: Callable

    def __str__(self) -> str:
        return self.name


@attrs.define
class Fields:
    # the downsize of doing this dynamically (i.e. make_dataclass) is no type hinting
    # so instead we keep it simple/verbose and just define the fields manually
    username = _Field("username", str_type_check)
    hashtag_name = _Field("hashtag_name", str_type_check)
    keyword = _Field("keyword", str_type_check)

    video_id = _Field("video_id", int_type_check)
    music_id = _Field("music_id", int_type_check)
    effect_id = _Field("effect_id", int_type_check)

    region_code = _Field("region_code", lambda x: x in SUPPORTED_REGION_CODES)
    video_length = _Field("video_length", lambda x: x in SUPPORTED_VIDEO_LENGTHS)

    create_date = _Field("create_date", validator=check_can_convert_date)


def convert_str_or_strseq_to_strseq(
    element_or_list: Union[str, Sequence[str]]
) -> Sequence[str]:
    """We purposely keep this function separate to the one optional_condition_or_list
    one below to avoid the edgecase that isinstance("any string", Sequence) = True
    """
    if isinstance(element_or_list, str):
        return [element_or_list]

    return element_or_list


@attrs.define
class Condition:
    field: _Field
    field_values: Union[str, Sequence[str]] = attrs.field(
        converter=convert_str_or_strseq_to_strseq
    )
    operation: str = attrs.field(validator=attrs.validators.in_(SUPPORTED_OPERATIONS))

    def __str__(self) -> str:
        str_field_values = ",".join([f'"{s}"' for s in self.field_values])

        return f"""{{"operation": "{self.operation}", "field_name": "{str(self.field.name)}", "field_values": [{str_field_values}]}}"""

    def as_dict(self) -> Mapping[str, Any]:
        return {
            "operation": self.operation,
            "field_name": self.field.name,
            "field_values": self.field_values,
        }


def format_conditions(
    conditions: Optional[Union[Sequence[Condition], Condition]],
    sep=",\n",
) -> str:
    """"""

    if conditions is None:
        return ""

    if isinstance(conditions, str):
        elements = str(conditions)

    else:
        assert isinstance(conditions, Sequence)
        elements = sep.join([str(cond) for cond in conditions])

    return f"""[\n{3*INDENT}{elements}\n{2*INDENT}]"""


def format_operand(name: str, operand: str, indent=INDENT) -> str:
    return f"""{indent}"{name}": {operand} """


def make_conditions_dict(
    conditions: Optional[Union[Sequence[Condition], Condition]],
) -> Mapping[str, Any]:
    if conditions is None:
        return None

    if isinstance(conditions, str):
        return conditions

    assert isinstance(conditions, Sequence)
    return [condition.as_dict() for condition in conditions]


OptionalCondOrCondSeq = Optional[Union[Condition, Sequence[Condition]]]


def convert_optional_cond_or_condseq_to_condseq(
    optional_cond_or_seq: OptionalCondOrCondSeq,
) -> Union[Sequence[Condition], None]:
    """"""

    if optional_cond_or_seq is None:
        return None

    elif isinstance(optional_cond_or_seq, Condition):
        return [optional_cond_or_seq]

    assert isinstance(optional_cond_or_seq, Sequence)
    return optional_cond_or_seq


@attrs.define
class Query:
    and_: OptionalCondOrCondSeq = attrs.field(
        default=None, converter=convert_optional_cond_or_condseq_to_condseq
    )
    or_: OptionalCondOrCondSeq = attrs.field(
        default=None, converter=convert_optional_cond_or_condseq_to_condseq
    )
    not_: OptionalCondOrCondSeq = attrs.field(
        default=None, converter=convert_optional_cond_or_condseq_to_condseq
    )

    def __attrs_post_init__(self):
        if self.and_ is None and self.or_ is None and self.not_ is None:
            raise ValueError("At least one of and_, or_, not_ must be passed!")

    def format_data(self) -> str:
        operand_names = ["and", "or", "not"]
        operand_values = [self.and_, self.or_, self.not_]

        all_operands = {}

        for name, val in zip(operand_names, operand_values):
            formatted_cond = format_conditions(val)
            if formatted_cond:
                all_operands[name] = formatted_cond

        formatted_operands = ",\n".join(
            [format_operand(name, val) for name, val in all_operands.items()]
        )

        return f""""query": {{\n{INDENT}{formatted_operands}\n{INDENT}}}"""

    def request_dict(self):
        operands = {"and": self.and_, "or": self.or_, "not": self.not_}
        formatted_operands = {}

        for name, val in operands.items():
            if val:
                formatted_operands[name] = make_conditions_dict(val)

        return formatted_operands

    def __str__(self) -> str:
        return self.format_data()


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

    def request_dict(self):
        ret = {
            "query": self.query.request_dict(),
            "max_count": self.max_count,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "is_random": self.is_random,
        }
        if self.search_id is not None:
            ret["search_id"] = self.search_id

        if self.cursor is not None:
            ret["cursor"] = self.cursor
        return ret

    # TODO(macpd): make custom JSONDecoder
    def request_json(self):
        return json.dumps(self.request_dict(), indent=2)

    def __str__(self) -> str:
        str_data = (
            f"""{{\n{INDENT}{self.query.format_data()},\n"""
            f"""{INDENT}"max_count": {self.max_count},\n"""
            f"""{INDENT}"start_date": "{self.start_date}",\n"""
            f"""{INDENT}"end_date": "{self.end_date}",\n"""
            f"""{INDENT}"is_random": {str(self.is_random).lower()}"""
        )

        if self.search_id is not None:
            str_data += f',\n    "search_id": "{self.search_id}"'

        if self.cursor is not None:
            str_data += f',\n    "cursor": {self.cursor}'

        str_data += "\n}"

        return str_data


@attrs.define
class TikTokApiRequestClient:
    _credentials: TiktokCredentials = attrs.field()
    _session: rq.Session = attrs.field()
    _raw_responses_output_dir: Optional[Path] = None

    @classmethod
    def from_credentials_file(
        cls, credentials_file: Path, *args, **kwargs
    ) -> TikTokApiRequestClient:
        with credentials_file.open("r") as f:
            dict_credentials = yaml.load(f, Loader=yaml.FullLoader)

        return cls(
            credentials=TiktokCredentials(
                dict_credentials["client_id"],
                dict_credentials["client_secret"],
                dict_credentials["client_key"],
            ),
            *args,
            **kwargs,
        )

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

        response = rq.post(
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

    @_session.default
    def _make_session(self):
        headers = {
            # We add the header here so the first run won't give us a InsecureRequestWarning
            # The token may time out which is why we manually add a hook to it
            "Authorization": f"Bearer {self._get_client_access_token()}",
            "Content-Type": "text/plain",
        }
        session = rq.Session()

        session.headers.update(headers)
        session.hooks["response"].append(self._refresh_token)
        session.verify = certifi.where()

        return session

    def _refresh_token(self, r, *args, **kwargs) -> rq.Response | None:
        # Adapted from https://stackoverflow.com/questions/37094419/python-requests-retry-request-after-re-authentication

        assert self._credentials is not None, "Credentials have not yet been set"

        if r.status_code == 401:
            logging.info("Fetching new token as the previous token expired")

            token = self._get_client_access_token()
            self._session.headers.update({"Authorization": f"Bearer {token}"})

            r.request.headers["Authorization"] = self._session.headers["Authorization"]

            return self._session.send(r.request)

    def _store_response(self, response: rq.Request) -> None:
        output_filename = self._raw_responses_output_dir / Path(
            str(pendulum.now("local").timestamp()) + ".json"
        )
        output_filename = output_filename.absolute()
        logging.info("Writing raw reponse to %s", output_filename)
        with output_filename.open("x") as f:
            f.write(response.text)

    def fetch(self, request: TiktokRequest) -> rq.Response:
        while True:
            try:
                api_response = self._post(request)
                return self.parse_response(api_response)
            except ApiRateLimitError as e:
                logging.warning("Response indicates rate limit exceeded: %r", e)
                sleep_until_next_utc_midnight()

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(10),
        wait=tenacity.wait_exponential(
            multiplier=1, min=3, max=timedelta(minutes=5).total_seconds()
        ),
        retry=tenacity.retry_if_exception_type(rq.RequestException),
        reraise=True,
    )
    def _post(self, request: TiktokRequest) -> rq.Response:
        #  data = str(request)
        data = request.request_json()
        logging.log(logging.INFO, f"Sending request with data: {data}")

        req = self._session.post(url=ALL_VIDEO_DATA_URL, data=data, verify=True)

        if self._raw_responses_output_dir is not None:
            self._store_response(req)

        if req.status_code == 200:
            return req

        if req.status_code == 429:
            raise ApiRateLimitError(repr(req))

        logging.log(
            logging.ERROR,
            f"Request failed, status code {req.status_code} - text {req.text} - data {data}",
        )
        req.raise_for_status()

    @staticmethod
    def parse_response(response: rq.Response) -> TikTokResponse:
        # TODO(macpd): re-request data if JSON decoding fails.
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


Cond = Condition
Op = Operations
F = Fields
