import pytest

@pytest.fixture
def test_query():
        hashtags = [
                "hashtag", "lol", "yay"
        ]

        return Query(
            and_=[
                Cond(Fields.hashtag_name, hashtags, Op.IN),
                Cond(Fields.region_code, "US", Op.EQ),
            ],
        )
