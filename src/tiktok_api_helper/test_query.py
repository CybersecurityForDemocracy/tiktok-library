import json
import pytest

from .query import Query, Cond, Op, Fields, QueryJSONEncoder


@pytest.fixture
def mock_query_us():
    hashtags = ["hashtag", "lol", "yay"]

    return Query(
        and_=[
            Cond(Fields.hashtag_name, hashtags, Op.IN),
            Cond(Fields.region_code, "US", Op.EQ),
        ],
    )


@pytest.fixture
def mock_query_us_ca():
    hashtags = ["hashtag", "lol", "yay"]

    return Query(
        and_=[
            Cond(Fields.hashtag_name, hashtags, Op.IN),
            Cond(Fields.region_code, ["US", "CA"], Op.IN),
        ],
    )


@pytest.fixture
def mock_query_exclude_some_hashtags():
    include_hashtags = ["hashtag", "lol", "yay"]
    exclude_hashtags = [
        "eww",
        "gross",
    ]

    return Query(
        and_=[
            Cond(Fields.hashtag_name, include_hashtags, Op.IN),
            Cond(Fields.region_code, ["US", "CA"], Op.IN),
        ],
        not_=[
            Cond(Fields.hashtag_name, exclude_hashtags, Op.IN),
        ],
    )

@pytest.fixture
def mock_query_create_date():
    return Query(
        and_=[
            Cond(Fields.create_date, "20230101", Op.EQ),
        ],
    )

def test_query_create_date(mock_query_create_date):
    assert mock_query_create_date.as_dict() == {
            'and': [{'field_name': "create_date", 'field_values': [ "20230101"], "operation": "EQ"}]}


def test_query_invalid_create_date():
    with pytest.raises(ValueError):
        Query(
            and_=[
                Cond(Fields.create_date, "2023-01-01", Op.EQ),
            ],
        )

    with pytest.raises(ValueError):
        Query(
            and_=[
                Cond(Fields.create_date, "It's not a date", Op.EQ),
            ],
        )

def test_query_us(mock_query_us):
    assert mock_query_us.as_dict() == {
        "and": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "hashtag",
                    "lol",
                    "yay",
                ],
                "operation": "IN",
            },
            {
                "field_name": "region_code",
                "field_values": [
                    "US",
                ],
                "operation": "EQ",
            },
        ],
    }


def test_query_us_ca(mock_query_us_ca):
    assert mock_query_us_ca.as_dict() == {
        "and": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "hashtag",
                    "lol",
                    "yay",
                ],
                "operation": "IN",
            },
            {
                "field_name": "region_code",
                "field_values": ["US", "CA"],
                "operation": "IN",
            },
        ],
    }


def test_query_exclude_some_hashtags(mock_query_exclude_some_hashtags):
    assert mock_query_exclude_some_hashtags.as_dict() == {
        "and": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "hashtag",
                    "lol",
                    "yay",
                ],
                "operation": "IN",
            },
            {
                "field_name": "region_code",
                "field_values": ["US", "CA"],
                "operation": "IN",
            },
        ],
        "not": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "eww",
                    "gross",
                ],
                "operation": "IN",
            },
        ],
    }

def test_invalid_query():
    with pytest.raises(ValueError):
        Query(
            and_=[
                Cond(Fields.region_code, "invalid", Op.EQ),
            ],
        )


def test_query_json_decoder_us(mock_query_us):
    assert (
        json.dumps(mock_query_us, cls=QueryJSONEncoder)
        == '{"and": [{"operation": "IN", "field_name": "hashtag_name", "field_values": ["hashtag", "lol", "yay"]}, {"operation": "EQ", "field_name": "region_code", "field_values": ["US"]}]}'
    )


def test_query_json_decoder_us_ca(mock_query_us_ca):
    assert (
        json.dumps(mock_query_us_ca, cls=QueryJSONEncoder)
        == '{"and": [{"operation": "IN", "field_name": "hashtag_name", "field_values": ["hashtag", "lol", "yay"]}, {"operation": "IN", "field_name": "region_code", "field_values": ["US", "CA"]}]}'
    )


def test_query_json_decoder_exclude_some_hashtags(mock_query_exclude_some_hashtags):
    assert (
        json.dumps(mock_query_exclude_some_hashtags, cls=QueryJSONEncoder)
        == '{"and": [{"operation": "IN", "field_name": "hashtag_name", "field_values": ["hashtag", "lol", "yay"]}, {"operation": "IN", "field_name": "region_code", "field_values": ["US", "CA"]}], "not": [{"operation": "IN", "field_name": "hashtag_name", "field_values": ["eww", "gross"]}]}'
    )
