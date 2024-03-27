from datetime import datetime
from typing import Callable, Optional, Sequence, Union, Mapping, Any
import json
import enum

import attrs

# TODO(macpd): tests

# fmt: off
SUPPORTED_OPERATIONS = ["EQ", "IN", "GT", "GTE", "LT", "LTE"]
SUPPORTED_REGION_CODES = [ 'FR', 'TH', 'MM', 'BD', 'IT', 'NP', 'IQ', 'BR', 'US', 'KW', 'VN', 'AR', 'KZ', 'GB', 'UA', 'TR', 'ID', 'PK', 'NG', 'KH', 'PH', 'EG', 'QA', 'MY', 'ES', 'JO', 'MA', 'SA', 'TW', 'AF', 'EC', 'MX', 'BW', 'JP', 'LT', 'TN', 'RO', 'LY', 'IL', 'DZ', 'CG', 'GH', 'DE', 'BJ', 'SN', 'SK', 'BY', 'NL', 'LA', 'BE', 'DO', 'TZ', 'LK', 'NI', 'LB', 'IE', 'RS', 'HU', 'PT', 'GP', 'CM', 'HN', 'FI', 'GA', 'BN', 'SG', 'BO', 'GM', 'BG', 'SD', 'TT', 'OM', 'FO', 'MZ', 'ML', 'UG', 'RE', 'PY', 'GT', 'CI', 'SR', 'AO', 'AZ', 'LR', 'CD', 'HR', 'SV', 'MV', 'GY', 'BH', 'TG', 'SL', 'MK', 'KE', 'MT', 'MG', 'MR', 'PA', 'IS', 'LU', 'HT', 'TM', 'ZM', 'CR', 'NO', 'AL', 'ET', 'GW', 'AU', 'KR', 'UY', 'JM', 'DK', 'AE', 'MD', 'SE', 'MU', 'SO', 'CO', 'AT', 'GR', 'UZ', 'CL', 'GE', 'PL', 'CA', 'CZ', 'ZA', 'AI', 'VE', 'KG', 'PE', 'CH', 'LV', 'PR', 'NZ', 'TL', 'BT', 'MN', 'FJ', 'SZ', 'VU', 'BF', 'TJ', 'BA', 'AM', 'TD', 'SI', 'CY', 'MW', 'EE', 'XK', 'ME', 'KY', 'YE', 'LS', 'ZW', 'MC', 'GN', 'BS', 'PF', 'NA', 'VI', 'BB', 'BZ', 'CW', 'PS', 'FM', 'PG', 'BI', 'AD', 'TV', 'GL', 'KM', 'AW', 'TC', 'CV', 'MO', 'VC', 'NE', 'WS', 'MP', 'DJ', 'RW', 'AG', 'GI', 'GQ', 'AS', 'AX', 'TO', 'KN', 'LC', 'NC', 'LI', 'SS', 'IR', 'SY', 'IM', 'SC', 'VG', 'SB', 'DM', 'KI', 'UM', 'SX', 'GD', 'MH', 'BQ', 'YT', 'ST', 'CF', 'BM', 'SM', 'PW', 'GU', 'HK', 'IN', 'CK', 'AQ', 'WF', 'JE', 'MQ', 'CN', 'GF', 'MS', 'GG', 'TK', 'FK', 'PM', 'NU', 'MF', 'ER', 'NF', 'VA', 'IO', 'SH', 'BL', 'CU', 'NR', 'TP', 'BV', 'EH', 'PN', 'TF', 'RU']
# The duration of the video SHORT: <15s MID: 15 ~60s LONG: 1~5min EXTRA_LONG: >5min
SUPPORTED_VIDEO_LENGTHS = ["SHORT", "MID", "LONG", "EXTRA_LONG"]
# fmt: on


class Operations(enum.StrEnum):
    EQ = "EQ"
    IN = "IN"
    GT = "GT"
    GTE = "GTE"
    LT = "LT"
    LTE = "LTE"


INDENT = "\t"


def build_check_type(expected_type) -> Callable[..., None]:

    def check_type(obj) -> None:
        assert isinstance(obj, expected_type)

    return check_type


str_type_check = build_check_type(str)
int_type_check = build_check_type(int)


def check_can_convert_date(string: str) -> None:
    # We check by directly trying to convert; will raise an error otherwise
    datetime.strptime(string, "%Y%m%d")


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

    if isinstance(optional_cond_or_seq, Condition):
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

    def as_request_dict(self):
        operands = {"and": self.and_, "or": self.or_, "not": self.not_}
        formatted_operands = {}

        for name, val in operands.items():
            if val:
                formatted_operands[name] = make_conditions_dict(val)

        return formatted_operands

    def __str__(self) -> str:
        return self.format_data()


class QueryJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Query):
            return o.as_request_dict()
        return super().default(o)

Cond = Condition
Op = Operations
F = Fields
