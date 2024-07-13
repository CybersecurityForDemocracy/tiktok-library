import json

import pytest

from tiktok_research_api_helper.query import (
    Cond,
    Fields,
    Op,
    VideoQuery,
    VideoQueryJSONEncoder,
    generate_query,
    get_normalized_hashtag_set,
    get_normalized_keyword_set,
    get_normalized_username_set,
)


@pytest.fixture
def mock_query_us():
    hashtags = ["hashtag", "lol", "yay"]

    return VideoQuery(
        and_=[
            Cond(Fields.hashtag_name, hashtags, Op.IN),
            Cond(Fields.region_code, "US", Op.EQ),
        ],
    )


@pytest.fixture
def mock_query_us_ca():
    hashtags = ["hashtag", "lol", "yay"]

    return VideoQuery(
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

    return VideoQuery(
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
    return VideoQuery(
        and_=[
            Cond(Fields.create_date, "20230101", Op.EQ),
        ],
    )


def test_query_create_date(mock_query_create_date):
    assert mock_query_create_date.as_dict() == {
        "and": [
            {
                "field_name": "create_date",
                "field_values": ["20230101"],
                "operation": "EQ",
            }
        ]
    }


def test_query_invalid_create_date():
    with pytest.raises(ValueError):
        VideoQuery(
            and_=[
                Cond(Fields.create_date, "2023-01-01", Op.EQ),
            ],
        )

    with pytest.raises(ValueError):
        VideoQuery(
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


def test_invalid_region_code():
    with pytest.raises(ValueError):
        VideoQuery(
            and_=[
                Cond(Fields.region_code, "invalid", Op.EQ),
            ],
        )


def test_query_json_decoder_us(mock_query_us):
    assert json.dumps(mock_query_us, cls=VideoQueryJSONEncoder, indent=1) == (
        """
{
 "and": [
  {
   "operation": "IN",
   "field_name": "hashtag_name",
   "field_values": [
    "hashtag",
    "lol",
    "yay"
   ]
  },
  {
   "operation": "EQ",
   "field_name": "region_code",
   "field_values": [
    "US"
   ]
  }
 ]
}
""".strip()
    )


def test_query_json_decoder_us_ca(mock_query_us_ca):
    assert json.dumps(mock_query_us_ca, cls=VideoQueryJSONEncoder, indent=1) == (
        """
{
 "and": [
  {
   "operation": "IN",
   "field_name": "hashtag_name",
   "field_values": [
    "hashtag",
    "lol",
    "yay"
   ]
  },
  {
   "operation": "IN",
   "field_name": "region_code",
   "field_values": [
    "US",
    "CA"
   ]
  }
 ]
}
""".strip()
    )


def test_query_json_decoder_exclude_some_hashtags(mock_query_exclude_some_hashtags):
    assert json.dumps(mock_query_exclude_some_hashtags, cls=VideoQueryJSONEncoder, indent=1) == (
        """
{
 "and": [
  {
   "operation": "IN",
   "field_name": "hashtag_name",
   "field_values": [
    "hashtag",
    "lol",
    "yay"
   ]
  },
  {
   "operation": "IN",
   "field_name": "region_code",
   "field_values": [
    "US",
    "CA"
   ]
  }
 ],
 "not": [
  {
   "operation": "IN",
   "field_name": "hashtag_name",
   "field_values": [
    "eww",
    "gross"
   ]
  }
 ]
}
""".strip()
    )


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("cheese", set(["cheese"])),
        ("cheese,cheese", set(["cheese"])),
        ("cheese,Cheese", set(["cheese"])),
        ("cheese,Cheese,CHEESE", set(["cheese"])),
        ("#cheese", set(["cheese"])),
        ("#cheese,cheese", set(["cheese"])),
        ("this,that,other", set(["this", "that", "other"])),
        ("#this,#that,#OTHER", set(["this", "that", "other"])),
        ("#this,#that,#OTHER,#other", set(["this", "that", "other"])),
    ],
)
def test_normalized_hashtag_set(test_input, expected):
    assert get_normalized_hashtag_set(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ("cheese", set(["cheese"])),
        ("cheese,cheese", set(["cheese"])),
        ("cheese,Cheese", set(["cheese"])),
        ("cheese,Cheese,CHEESE", set(["cheese"])),
        ("this,that,other", set(["this", "that", "other"])),
        ("this,that,OTHER", set(["this", "that", "other"])),
        ("other,this,other,that,OTHER", set(["this", "that", "other"])),
    ],
)
def test_normalized_keyword_set(test_input, expected):
    assert get_normalized_keyword_set(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ("cheese", set(["cheese"])),
        ("cheese,cheese", set(["cheese"])),
        ("cheese,Cheese", set(["cheese"])),
        ("cheese,Cheese,CHEESE", set(["cheese"])),
        ("@cheese", set(["cheese"])),
        ("@cheese,cheese", set(["cheese"])),
        ("this,that,other", set(["this", "that", "other"])),
        ("@this,that@,@OTHER", set(["this", "that", "other"])),
        ("@this,@that,@OTHER,other@", set(["this", "that", "other"])),
    ],
)
def test_normalized_username_set(test_input, expected):
    assert get_normalized_username_set(test_input) == expected


def test_generate_query_include_any_hashtags():
    assert generate_query(include_any_hashtags="this,that,other").as_dict() == {
        "and": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "other",
                    "that",
                    "this",
                ],
                "operation": "IN",
            }
        ]
    }


def test_generate_query_include_all_hashtags():
    assert generate_query(include_all_hashtags="this,that,other").as_dict() == {
        "and": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "that",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ]
    }


def test_generate_query_exclude_any_hashtags():
    assert generate_query(exclude_any_hashtags="this,that,other").as_dict() == {
        "not": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "other",
                    "that",
                    "this",
                ],
                "operation": "IN",
            }
        ]
    }


def test_generate_query_exclude_all_hashtags():
    assert generate_query(exclude_all_hashtags="this,that,other").as_dict() == {
        "not": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "that",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ]
    }


def test_generate_query_only_from_usernames():
    assert generate_query(only_from_usernames="mark,sally,kai").as_dict() == {
        "and": [
            {
                "field_name": "username",
                "field_values": [
                    "kai",
                    "mark",
                    "sally",
                ],
                "operation": "IN",
            }
        ]
    }


def test_generate_query_exclude_from_usernames():
    assert generate_query(exclude_from_usernames="mark,sally,kai").as_dict() == {
        "not": [
            {
                "field_name": "username",
                "field_values": [
                    "kai",
                    "mark",
                    "sally",
                ],
                "operation": "IN",
            }
        ]
    }


def test_generate_query_include_all_hashtags_with_exclude_any_hashtags():
    assert generate_query(
        include_all_hashtags="this,that,other", exclude_any_hashtags="cheese,butter"
    ).as_dict() == {
        "and": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "that",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ],
        "not": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "butter",
                    "cheese",
                ],
                "operation": "IN",
            }
        ],
    }


def test_generate_query_include_any_hashtags_with_exclude_all_hashtags():
    assert generate_query(
        include_any_hashtags="this,that,other", exclude_all_hashtags="cheese,butter"
    ).as_dict() == {
        "and": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "other",
                    "that",
                    "this",
                ],
                "operation": "IN",
            },
        ],
        "not": [
            {
                "field_name": "hashtag_name",
                "field_values": ["butter"],
                "operation": "EQ",
            },
            {
                "field_name": "hashtag_name",
                "field_values": ["cheese"],
                "operation": "EQ",
            },
        ],
    }


def test_generate_query_include_any_hashtags_with_exclude_any_hashtags():
    assert generate_query(
        include_any_hashtags="this,that,other", exclude_any_hashtags="cheese,butter"
    ).as_dict() == {
        "and": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "other",
                    "that",
                    "this",
                ],
                "operation": "IN",
            },
        ],
        "not": [
            {
                "field_name": "hashtag_name",
                "field_values": ["butter", "cheese"],
                "operation": "IN",
            },
        ],
    }


def test_generate_query_include_all_hashtags_with_exclude_all_hashtags():
    assert generate_query(
        include_all_hashtags="this,that,other", exclude_all_hashtags="cheese,butter"
    ).as_dict() == {
        "and": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {"field_name": "hashtag_name", "field_values": ["that"], "operation": "EQ"},
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ],
        "not": [
            {
                "field_name": "hashtag_name",
                "field_values": ["butter"],
                "operation": "EQ",
            },
            {
                "field_name": "hashtag_name",
                "field_values": ["cheese"],
                "operation": "EQ",
            },
        ],
    }


def test_generate_query_include_any_keywords():
    assert generate_query(include_any_keywords="this,that,other").as_dict() == {
        "and": [
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                    "that",
                    "this",
                ],
                "operation": "IN",
            }
        ]
    }


def test_generate_query_include_all_keywords():
    assert generate_query(include_all_keywords="this,that,other").as_dict() == {
        "and": [
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "that",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ]
    }


def test_generate_query_exclude_any_keywords():
    assert generate_query(exclude_any_keywords="this,that,other").as_dict() == {
        "not": [
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                    "that",
                    "this",
                ],
                "operation": "IN",
            }
        ]
    }


def test_generate_query_exclude_all_keywords():
    assert generate_query(exclude_all_keywords="this,that,other").as_dict() == {
        "not": [
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "that",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ]
    }


def test_generate_query_include_all_keywords_with_exclude_any_keywords():
    assert generate_query(
        include_all_keywords="this,that,other", exclude_any_keywords="cheese,butter"
    ).as_dict() == {
        "and": [
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "that",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ],
        "not": [
            {
                "field_name": "keyword",
                "field_values": [
                    "butter",
                    "cheese",
                ],
                "operation": "IN",
            }
        ],
    }


def test_generate_query_include_any_keywords_with_exclude_all_keywords():
    assert generate_query(
        include_any_keywords="this,that,other", exclude_all_keywords="cheese,butter"
    ).as_dict() == {
        "and": [
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                    "that",
                    "this",
                ],
                "operation": "IN",
            },
        ],
        "not": [
            {"field_name": "keyword", "field_values": ["butter"], "operation": "EQ"},
            {"field_name": "keyword", "field_values": ["cheese"], "operation": "EQ"},
        ],
    }


def test_generate_query_include_all_keywords_with_exclude_all_keywords():
    assert generate_query(
        include_all_keywords="this,that,other", exclude_all_keywords="cheese,butter"
    ).as_dict() == {
        "and": [
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {"field_name": "keyword", "field_values": ["that"], "operation": "EQ"},
            {
                "field_name": "keyword",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ],
        "not": [
            {"field_name": "keyword", "field_values": ["butter"], "operation": "EQ"},
            {"field_name": "keyword", "field_values": ["cheese"], "operation": "EQ"},
        ],
    }


# Test combinations of include/exclude keywords and hashtags


def test_generate_query_include_all_keywords_with_include_any_hashtags():
    assert generate_query(
        include_all_keywords="this,that,other", include_any_hashtags="cheese,butter"
    ).as_dict() == {
        "and": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "butter",
                    "cheese",
                ],
                "operation": "IN",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "that",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ],
    }


def test_generate_query_include_all_keywords_with_include_all_hashtags():
    assert generate_query(
        include_all_keywords="this,that,other", include_all_hashtags="cheese,butter"
    ).as_dict() == {
        "and": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "butter",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "cheese",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "that",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ],
    }


def test_generate_query_include_any_keywords_with_include_any_hashtags():
    assert generate_query(
        include_any_keywords="this,that,other", include_any_hashtags="cheese,butter"
    ).as_dict() == {
        "and": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "butter",
                    "cheese",
                ],
                "operation": "IN",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                    "that",
                    "this",
                ],
                "operation": "IN",
            },
        ]
    }


def test_generate_query_exclude_all_keywords_with_exclude_any_hashtags():
    assert generate_query(
        exclude_all_keywords="this,that,other", exclude_any_hashtags="cheese,butter"
    ).as_dict() == {
        "not": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "butter",
                    "cheese",
                ],
                "operation": "IN",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "that",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ],
    }


def test_generate_query_exclude_all_keywords_with_exclude_all_hashtags():
    assert generate_query(
        exclude_all_keywords="this,that,other", exclude_all_hashtags="cheese,butter"
    ).as_dict() == {
        "not": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "butter",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "cheese",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "that",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ],
    }


def test_generate_query_exclude_any_keywords_with_exclude_any_hashtags():
    assert generate_query(
        exclude_any_keywords="this,that,other", exclude_any_hashtags="cheese,butter"
    ).as_dict() == {
        "not": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "butter",
                    "cheese",
                ],
                "operation": "IN",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                    "that",
                    "this",
                ],
                "operation": "IN",
            },
        ]
    }


def test_generate_query_include_all_keywords_with_exclude_any_hashtags():
    assert generate_query(
        include_all_keywords="this,that,other", exclude_any_hashtags="cheese,butter"
    ).as_dict() == {
        "and": [
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "that",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "keyword",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ],
        "not": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "butter",
                    "cheese",
                ],
                "operation": "IN",
            }
        ],
    }


def test_generate_query_include_all_hashtags_with_exclude_any_keywords():
    assert generate_query(
        include_all_hashtags="this,that,other", exclude_any_keywords="cheese,butter"
    ).as_dict() == {
        "and": [
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "other",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "that",
                ],
                "operation": "EQ",
            },
            {
                "field_name": "hashtag_name",
                "field_values": [
                    "this",
                ],
                "operation": "EQ",
            },
        ],
        "not": [
            {
                "field_name": "keyword",
                "field_values": [
                    "butter",
                    "cheese",
                ],
                "operation": "IN",
            }
        ],
    }


def test_generate_query_include_any_keywords_with_exclude_any_keywords_with_only_from_usernames():
    assert generate_query(
        include_any_keywords="this,that,other",
        exclude_any_keywords="cheese,butter",
        only_from_usernames="mark,sally,kai",
    ).as_dict() == {
        "and": [
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                    "that",
                    "this",
                ],
                "operation": "IN",
            },
            {
                "field_name": "username",
                "field_values": [
                    "kai",
                    "mark",
                    "sally",
                ],
                "operation": "IN",
            },
        ],
        "not": [
            {
                "field_name": "keyword",
                "field_values": ["butter", "cheese"],
                "operation": "IN",
            },
        ],
    }


def test_generate_query_include_any_keywords_with_exclude_any_keywords_with_exclude_from_usernames():  # noqa E501 Unfortunately long test name is too long for line
    assert generate_query(
        include_any_keywords="this,that,other",
        exclude_any_keywords="cheese,butter",
        exclude_from_usernames="mark,sally,kai",
    ).as_dict() == {
        "and": [
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                    "that",
                    "this",
                ],
                "operation": "IN",
            },
        ],
        "not": [
            {
                "field_name": "keyword",
                "field_values": ["butter", "cheese"],
                "operation": "IN",
            },
            {
                "field_name": "username",
                "field_values": [
                    "kai",
                    "mark",
                    "sally",
                ],
                "operation": "IN",
            },
        ],
    }


def test_generate_query_include_any_keywords_with_exclude_any_keywords_with_only_from_usernames_with_exclude_from_usernames():  # noqa E501 Unfortunately long test name is too long for line
    assert generate_query(
        include_any_keywords="this,that,other",
        exclude_any_keywords="cheese,butter",
        only_from_usernames="amuro,roux",
        exclude_from_usernames="mark,sally,kai",
    ).as_dict() == {
        "and": [
            {
                "field_name": "keyword",
                "field_values": [
                    "other",
                    "that",
                    "this",
                ],
                "operation": "IN",
            },
            {
                "field_name": "username",
                "field_values": ["amuro", "roux"],
                "operation": "IN",
            },
        ],
        "not": [
            {
                "field_name": "keyword",
                "field_values": ["butter", "cheese"],
                "operation": "IN",
            },
            {
                "field_name": "username",
                "field_values": [
                    "kai",
                    "mark",
                    "sally",
                ],
                "operation": "IN",
            },
        ],
    }
