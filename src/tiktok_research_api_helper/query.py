import enum
import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import attrs

from tiktok_research_api_helper import utils
from tiktok_research_api_helper.region_codes import SupportedRegions

_QUERY_AND_ARG_NAME = "and_"
_QUERY_NOT_ARG_NAME = "not_"


class Operations(enum.StrEnum):
    EQ = "EQ"
    IN = "IN"
    GT = "GT"
    GTE = "GTE"
    LT = "LT"
    LTE = "LTE"


def check_can_convert_date(inst, attr, value: str) -> None:
    # We check by directly trying to convert; will raise an error otherwise
    utils.str_tiktok_date_format_to_datetime(value)


# The duration of the video SHORT: <15s MID: 15 ~60s LONG: 1~5min EXTRA_LONG: >5min
class VideoLength(enum.StrEnum):
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
    username = _Field("username", validator=attrs.validators.instance_of(str))
    hashtag_name = _Field("hashtag_name", validator=attrs.validators.instance_of(str))
    keyword = _Field("keyword", validator=attrs.validators.instance_of(str))

    video_id = _Field("video_id", validator=attrs.validators.instance_of(str))
    music_id = _Field("music_id", validator=attrs.validators.instance_of(str))
    effect_id = _Field("effect_id", validator=attrs.validators.instance_of(str))

    region_code = _Field(
        "region_code",
        validator=attrs.validators.in_({region.value for region in SupportedRegions}),
    )
    video_length = _Field("video_length", validator=attrs.validators.in_(VideoLength))

    create_date = _Field("create_date", validator=check_can_convert_date)


def convert_str_or_strseq_to_strseq(element_or_list: str | Sequence[str]) -> Sequence[str]:
    """We purposely keep this function separate to the one optional_condition_or_list
    one below to avoid the edgecase that isinstance("any string", Sequence) = True
    """
    if isinstance(element_or_list, str):
        return [element_or_list]

    return element_or_list


@attrs.define
class Condition:
    field: _Field
    field_values: str | Sequence[str] = attrs.field(
        converter=convert_str_or_strseq_to_strseq,
        validator=attrs.validators.instance_of((str, Sequence)),
    )
    operation: str = attrs.field(validator=attrs.validators.in_(Operations))

    @field_values.validator  # type: ignore - attrs does not support type hinting for validators
    def validate_field_values(self, attribute, value):
        for elem in value:
            self.field.validator(inst=self, attr=attribute, value=elem)

    def as_dict(self) -> Mapping[str, Any]:
        return {
            "operation": str(self.operation),
            "field_name": self.field.name,
            "field_values": self.field_values,
        }


def make_conditions_dict(
    conditions: Sequence[Condition] | Condition | None,
) -> Sequence[Mapping[str, Any]] | None | str:
    if conditions is None:
        return None

    if isinstance(conditions, str):
        return conditions

    assert isinstance(conditions, Sequence)
    return [condition.as_dict() for condition in conditions]


OptionalCondOrCondSeq = Condition | Sequence[Condition] | None


def convert_optional_cond_or_condseq_to_condseq(
    optional_cond_or_seq: OptionalCondOrCondSeq,
) -> Sequence[Condition] | None:
    """"""

    if optional_cond_or_seq is None:
        return None

    if isinstance(optional_cond_or_seq, Condition):
        return [optional_cond_or_seq]

    assert isinstance(optional_cond_or_seq, Sequence)
    return optional_cond_or_seq


@attrs.define
class VideoQuery:
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

    def as_dict(self):
        operands = {"and": self.and_, "or": self.or_, "not": self.not_}
        formatted_operands = {}

        for name, val in operands.items():
            if val:
                formatted_operands[name] = make_conditions_dict(val)

        return formatted_operands


class VideoQueryJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, VideoQuery):
            return o.as_dict()
        return super().default(o)


def get_normalized_hashtag_set(comma_separated_hashtags: str) -> set[str]:
    """Takes a string of comma separated hashtag names and returns a set of hashtag names all
    lowercase and stripped of leading "#" if present."""
    return {hashtag.lstrip("#").lower() for hashtag in comma_separated_hashtags.split(",")}


def get_normalized_keyword_set(comma_separated_keywords: str) -> set[str]:
    """Takes a string of comma separated keywords and returns a set of keywords all lowercase"""
    return {keyword.lower() for keyword in comma_separated_keywords.split(",")}


def get_normalized_username_set(comma_separated_usernames: str) -> set[str]:
    """Takes a string of comma separated usernames and returns a set of usernames all lowercase with
    any @ symbols remove"""
    return {username.strip("@").lower() for username in comma_separated_usernames.split(",")}


def any_hashtags_condition(hashtags):
    return Cond(Fields.hashtag_name, sorted(get_normalized_hashtag_set(hashtags)), Op.IN)


def all_hashtags_condition_list(hashtags):
    return [
        Cond(Fields.hashtag_name, hashtag_name, Op.EQ)
        for hashtag_name in sorted(get_normalized_hashtag_set(hashtags))
    ]


def any_keywords_condition(keywords):
    return Cond(Fields.keyword, sorted(get_normalized_keyword_set(keywords)), Op.IN)


def all_keywords_condition_list(keywords):
    return [
        Cond(Fields.keyword, keyword, Op.EQ)
        for keyword in sorted(get_normalized_keyword_set(keywords))
    ]


def any_usernames_condition(usernames):
    return Cond(Fields.username, sorted(get_normalized_username_set(usernames)), Op.IN)


def generate_query(
    region_codes: list[SupportedRegions] | None = None,
    include_any_hashtags: str | None = None,
    include_all_hashtags: str | None = None,
    exclude_any_hashtags: str | None = None,
    exclude_all_hashtags: str | None = None,
    include_any_keywords: str | None = None,
    include_all_keywords: str | None = None,
    exclude_any_keywords: str | None = None,
    exclude_all_keywords: str | None = None,
    only_from_usernames: str | None = None,
    exclude_from_usernames: str | None = None,
) -> VideoQuery:
    query_args = {_QUERY_AND_ARG_NAME: [], _QUERY_NOT_ARG_NAME: []}

    if include_any_hashtags:
        query_args[_QUERY_AND_ARG_NAME].append(any_hashtags_condition(include_any_hashtags))
    elif include_all_hashtags:
        query_args[_QUERY_AND_ARG_NAME].extend(all_hashtags_condition_list(include_all_hashtags))

    if exclude_any_hashtags:
        query_args[_QUERY_NOT_ARG_NAME].append(any_hashtags_condition(exclude_any_hashtags))
    elif exclude_all_hashtags:
        query_args[_QUERY_NOT_ARG_NAME].extend(all_hashtags_condition_list(exclude_all_hashtags))

    if include_any_keywords:
        query_args[_QUERY_AND_ARG_NAME].append(any_keywords_condition(include_any_keywords))
    elif include_all_keywords:
        query_args[_QUERY_AND_ARG_NAME].extend(all_keywords_condition_list(include_all_keywords))

    if exclude_any_keywords:
        query_args[_QUERY_NOT_ARG_NAME].append(any_keywords_condition(exclude_any_keywords))
    elif exclude_all_keywords:
        query_args[_QUERY_NOT_ARG_NAME].extend(all_keywords_condition_list(exclude_all_keywords))

    if only_from_usernames:
        query_args[_QUERY_AND_ARG_NAME].append(any_usernames_condition(only_from_usernames))

    if exclude_from_usernames:
        query_args[_QUERY_NOT_ARG_NAME].append(any_usernames_condition(exclude_from_usernames))

    if region_codes:
        query_args[_QUERY_AND_ARG_NAME].append(
            Cond(Fields.region_code, sorted(region_codes), Op.IN)
        )

    return VideoQuery(**query_args)


Cond = Condition
Op = Operations
F = Fields
