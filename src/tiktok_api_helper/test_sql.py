import datetime

from sqlalchemy import (
    Engine,
    select,
)
from sqlalchemy.orm import Session
import pytest
import json

from .sql import (
    Crawl,
    Video,
    Hashtag,
    get_engine_and_create_tables,
    Base,
    upsert_videos,
)

_IN_MEMORY_SQLITE_DATABASE_URL = "sqlite://"


@pytest.fixture
def test_database_engine(database_url_command_line_arg) -> Engine:
    if database_url_command_line_arg:
        database_url = database_url_command_line_arg
    else:
        database_url = _IN_MEMORY_SQLITE_DATABASE_URL

    engine = get_engine_and_create_tables(database_url, echo=True)
    yield engine
    # Clear database after test runs and releases fixture
    Base.metadata.drop_all(engine)


@pytest.fixture
def mock_videos():
    now = datetime.datetime.now()
    return [
        Video(
            id=1,
            username="Testing1",
            region_code="US",
            create_time=now,
            hashtags=[Hashtag(name="hashtag1"), Hashtag(name="hashtag2")],
        ),
        Video(
            id=2,
            username="Testing2",
            region_code="US",
            comment_count=1,
            create_time=now,
            effect_ids=[1, 2, 3],
            hashtags=[Hashtag(name="Hello"), Hashtag(name="World")],
            playlist_id=7044254287731739397,
            voice_to_text="a string",
            extra_data={"some-future-field-i-havent-thought-of": ["value"]},
            source=["testing"],
        ),
    ]


@pytest.fixture
def api_response_videos():
    with open("src/tiktok_api_helper/testdata/api_response.json", "r") as f:
        return json.loads(f.read())["data"]["videos"]


@pytest.fixture
def mock_crawl():
    return Crawl(
        cursor=1, has_more=False, search_id="test", query="test", source=["testing"]
    )


def assert_video_database_object_list_matches_api_responses_dict(
    video_objects, api_responses_video_dict
):
    video_id_to_database_object = {video.id: video for video in video_objects}
    video_id_to_api_response_dict = {
        api_response_dict["id"]: api_response_dict
        for api_response_dict in api_responses_video_dict
    }
    database_video_ids = set(video_id_to_database_object.keys())
    api_responses_video_ids = set(video_id_to_api_response_dict.keys())
    assert (
        database_video_ids == api_responses_video_ids
    ), f"Database objects missing IDs in API response ({api_responses_video_ids - database_video_ids}). API responses missing IDs in database objects ({database_video_ids - api_responses_video_ids})"
    for video_id in database_video_ids:
        _assert_video_database_object_matches_api_response_dict(
            video_id_to_database_object[video_id],
            video_id_to_api_response_dict[video_id],
        )


def _assert_video_database_object_matches_api_response_dict(
    video_object, api_response_video_dict
):
    for k, v in api_response_video_dict.items():
        try:
            db_value = getattr(video_object, k)
            if isinstance(db_value, datetime.datetime):
                db_value = db_value.timestamp()
            if isinstance(db_value, list):
                db_value.sort()
                v.sort()

            assert db_value == v, (
                f"Video object {video_object!r} attribute {k} value {getattr(video_object, k)} != "
                f"API response dict value {v}; full API response dict:\n{api_response_video_dict}"
            )

        except AttributeError as e:
            raise ValueError(
                f"Video object {video_object!r} has not attribute {k}: {e}"
            )


def test_video_basic_insert(test_database_engine, mock_videos):
    with Session(test_database_engine) as session:
        session.add_all(mock_videos)
        session.commit()
        assert session.scalars(select(Video).order_by(Video.id)).all() == mock_videos


def test_crawl_basic_insert(test_database_engine, mock_crawl):
    with Session(test_database_engine) as session:
        mock_crawl = Crawl(
            cursor=1, has_more=False, search_id="test", query="test", source=["testing"]
        )
        session.add_all([mock_crawl])
        session.commit()

        assert session.scalars(select(Crawl).order_by(Crawl.id)).all() == [mock_crawl]


def test_upsert(test_database_engine, mock_videos, mock_crawl):
    with Session(test_database_engine) as session:
        session.add_all(mock_videos)
        session.commit()

        session.add_all([mock_crawl])
        session.commit()
        assert session.scalars(select(Video.source).order_by(Video.id)).all() == [
            mock_videos[0].source,
            mock_videos[1].source,
        ]
        assert session.scalars(select(Video.share_count).order_by(Video.id)).all() == [
            None,
            None,
        ]

        new_source = ["0.0-testing"]
        upsert_videos(
            [
                {
                    "id": mock_videos[0].id,
                    "share_count": 300,
                    "create_time": datetime.datetime.utcnow().timestamp(),
                },
                {
                    "id": mock_videos[1].id,
                    "share_count": 3,
                    "create_time": datetime.datetime.utcnow().timestamp(),
                },
            ],
            source=new_source,
            engine=test_database_engine,
        )
        assert session.scalars(select(Video.source).order_by(Video.id)).all() == [
            mock_videos[0].source + new_source,
            mock_videos[1].source + new_source,
        ]
        assert session.scalars(select(Video.share_count).order_by(Video.id)).all() == [
            300,
            3,
        ]


def test_upsert_existing_hashtags_names_gets_same_id(
    test_database_engine,
    mock_videos,
):
    """Tests that adding a video with an existing hashtag name (from a previously added video)
    succeeds, gets the same ID it had previously, and does not raise a Unique violation error.
    """
    utcnow = datetime.datetime.utcnow().timestamp()
    with Session(test_database_engine) as session:
        session.commit()

        new_source = ["0.0-testing"]
        upsert_videos(
            [
                {
                    "id": 0,
                    "hashtag_names": ["hashtag1", "hashtag2"],
                    "create_time": utcnow,
                    "username": "user0",
                    "region_code": "US",
                },
            ],
            source=new_source,
            engine=test_database_engine,
        )

        session.expire_all()

        original_hashtags = {
            hashtag.id: hashtag.name
            for hashtag in session.scalars(select(Hashtag).order_by(Hashtag.name)).all()
        }
        assert set(original_hashtags.values()) == {"hashtag1", "hashtag2"}

        upsert_videos(
            [
                {
                    "id": 1,
                    "hashtag_names": ["hashtag1", "hashtag2", "hashtag3"],
                    "create_time": utcnow,
                    "username": "user1",
                    "region_code": "US",
                },
            ],
            source=new_source,
            engine=test_database_engine,
        )

        session.expire_all()

        assert {
            hashtag.id: hashtag.name
            for hashtag in session.scalars(
                select(Hashtag)
                .where(Hashtag.name.in_(["hashtag1", "hashtag2"]))
                .order_by(Hashtag.name)
            ).all()
        } == original_hashtags

        # Confirm mapping of hashtag IDs -> video IDs is correct
        assert [
            (v.id, {*v.hashtag_names})
            for v in session.scalars(select(Video).order_by(Video.id)).all()
        ] == [
            (0, {"hashtag1", "hashtag2"}),
            (1, {"hashtag1", "hashtag2", "hashtag3"}),
        ]


def test_upsert_updates_existing_and_inserts_new_video_data(
    test_database_engine,
    mock_videos,
    api_response_videos,
):
    utcnow = datetime.datetime.utcnow().timestamp()
    with Session(test_database_engine) as session:
        session.add_all(mock_videos)
        session.commit()

        new_source = ["0.0-testing"]
        upsert_videos(
            [
                api_response_videos[0],
                {
                    "id": mock_videos[1].id,
                    "comment_count": mock_videos[1].comment_count + 1,
                    "create_time": utcnow,
                    "hashtag_names": ["hashtag1", "hashtag2"],
                },
            ],
            source=new_source,
            engine=test_database_engine,
        )
        assert session.execute(
            select(Video.id, Video.comment_count, Video.create_time).order_by(Video.id)
        ).all() == [
            (mock_videos[0].id, None, mock_videos[0].create_time),
            (
                mock_videos[1].id,
                mock_videos[1].comment_count + 1,
                datetime.datetime.fromtimestamp(utcnow),
            ),
            (
                api_response_videos[0]["id"],
                api_response_videos[0]["comment_count"],
                datetime.datetime.fromtimestamp(api_response_videos[0]["create_time"]),
            ),
        ]


def test_upsert_updates_existing_and_inserts_new_video_data_and_hashtag_names(
    test_database_engine,
    mock_videos,
    api_response_videos,
):
    utcnow = datetime.datetime.utcnow().timestamp()
    with Session(test_database_engine) as session:
        session.add_all(mock_videos)
        session.commit()

        new_source = ["0.0-testing"]
        upsert_videos(
            [
                api_response_videos[0],
                {
                    "id": mock_videos[1].id,
                    "comment_count": mock_videos[1].comment_count + 1,
                    "create_time": utcnow,
                    "hashtag_names": ["hashtag1", "hashtag2"],
                },
            ],
            source=new_source,
            engine=test_database_engine,
        )

        assert sorted(session.scalars(select(Hashtag.name)).all()) == [
            "Hello",
            "World",
            "cats",
            "duet",
            "hashtag1",
            "hashtag2",
        ]

        assert [
            (v.id, {*v.hashtag_names})
            for v in session.scalars(select(Video).order_by(Video.id)).all()
        ] == [
            (mock_videos[0].id, {"hashtag1", "hashtag2"}),
            (mock_videos[1].id, {"hashtag1", "hashtag2"}),
            (api_response_videos[0]["id"], {*api_response_videos[0]["hashtag_names"]}),
        ]


def test_upsert_api_response_videos(test_database_engine, api_response_videos):
    with Session(test_database_engine) as session:
        upsert_videos(api_response_videos, test_database_engine)
        assert_video_database_object_list_matches_api_responses_dict(
            session.scalars(select(Video)).all(), api_response_videos
        )


def test_remove_all(test_database_engine, mock_videos, mock_crawl):
    with Session(test_database_engine) as session:
        session.add_all(mock_videos)
        session.add_all([mock_crawl])
        session.commit()
        assert session.scalars(select(Video).order_by(Video.id)).all() == mock_videos
        assert session.scalars(select(Crawl)).all() == [mock_crawl]

        for video in session.scalars(select(Video)):
            print("Deleting video", video)
            session.delete(video)

        for crawl in session.scalars(select(Crawl)):
            print("Deleting crawl", crawl)
            session.delete(crawl)

        session.commit()
        assert session.scalars(select(Video).order_by(Video.id)).all() == []
        assert session.scalars(select(Crawl)).all() == []
