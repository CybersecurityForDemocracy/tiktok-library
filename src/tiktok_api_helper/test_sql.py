import datetime

from sqlalchemy import (
    Engine,
    select,
)
from sqlalchemy.orm import Session
import pytest

from .sql import (
    Crawl,
    Video,
    Hashtag,
    get_engine_and_create_tables,
    Base,
    upsert_videos,
)

MOCK_VIDEO_DATA = {
    "like_count": 760,
    "music_id": 6833934234948732941,
    "username": "himom",
    "video_description": "look at this silly dance #silly #dance",
    "view_count": 19365,
    "comment_count": 373,
    "effect_ids": ["0"],
    "id": 7094037673978973446,
    "region_code": "US",
    "share_count": 30,
    "create_time": 1651709360,
    "hashtag_names": ["silly", "dance"],
}


# TODO(macpd): create integration test with postgres, perhaps using postgres docker container

@pytest.fixture
def in_memory_database() -> Engine:
    engine = get_engine_and_create_tables("sqlite://", echo=True)
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
def mock_crawl():
    return Crawl(
        cursor=1, has_more=False, search_id="test", query="test", source=["testing"]
    )


def test_video_basic_insert(in_memory_database, mock_videos):
    with Session(in_memory_database) as session:
        session.add_all(mock_videos)
        session.commit()
        assert session.scalars(select(Video).order_by(Video.id)).all() == mock_videos


def test_crawl_basic_insert(in_memory_database, mock_crawl):
    with Session(in_memory_database) as session:
        mock_crawl = Crawl(
            cursor=1, has_more=False, search_id="test", query="test", source=["testing"]
        )
        session.add_all([mock_crawl])
        session.commit()

        assert session.scalars(select(Crawl).order_by(Crawl.id)).all() == [mock_crawl]


def test_upsert(in_memory_database, mock_videos, mock_crawl):
    with Session(in_memory_database) as session:
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
            engine=in_memory_database,
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
    in_memory_database,
    mock_videos,
):
    """Tests that adding a video with an existing hashtag name (from a previously added video)
    succeeds, gets the same ID it had previously, and does not raise a Unique violation error.
    """
    utcnow = datetime.datetime.utcnow().timestamp()
    with Session(in_memory_database) as session:
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
            engine=in_memory_database,
        )
        original_hashtags = session.scalars(
            select(Hashtag.id, Hashtag.name).order_by(Hashtag.name)
        ).all()
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
            engine=in_memory_database,
        )
        assert (
            session.scalars(
                select(Hashtag.id, Hashtag.name)
                .where(Hashtag.name.in_(["hashtag1", "hashtag2"]))
                .order_by(Hashtag.name)
            ).all()
            == original_hashtags
        )

        # Confirm mapping of hashtag IDs -> video IDs is correct
        assert session.execute(
            select(Video.id, Hashtag.name).join(Video.hashtags).order_by(Video.id)
        ).all() == [
            (
                0,
                "hashtag1",
            ),
            (0, "hashtag2"),
            (
                1,
                "hashtag1",
            ),
            (1, "hashtag2"),
            (1, "hashtag3"),
        ]


def test_upsert_updates_existing_and_inserts_new_video_data(
    in_memory_database,
    mock_videos,
):
    utcnow = datetime.datetime.utcnow().timestamp()
    with Session(in_memory_database) as session:
        session.add_all(mock_videos)
        session.commit()

        new_source = ["0.0-testing"]
        upsert_videos(
            [
                MOCK_VIDEO_DATA,
                {
                    "id": mock_videos[1].id,
                    "comment_count": mock_videos[1].comment_count + 1,
                    "create_time": utcnow,
                    "hashtag_names": ["hashtag1", "hashtag2"],
                },
            ],
            source=new_source,
            engine=in_memory_database,
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
                MOCK_VIDEO_DATA["id"],
                MOCK_VIDEO_DATA["comment_count"],
                datetime.datetime.fromtimestamp(MOCK_VIDEO_DATA["create_time"]),
            ),
        ]

        video_in_db = session.scalars(
            select(Video).where(Video.id == MOCK_VIDEO_DATA["id"])
        ).first()
        for k, v in MOCK_VIDEO_DATA.items():
            if k == "hashtag_names":
                video_in_db_value = [hashtag.name for hashtag in video_in_db.hashtags]
            else:
                video_in_db_value = getattr(video_in_db, k)

            if isinstance(video_in_db_value, datetime.datetime):
                assert (
                    int(video_in_db_value.timestamp()) == v
                ), f"{k} field does not match"
            else:
                assert video_in_db_value == v, f"{k} field does not match"

        assert sorted(session.scalars(select(Hashtag.name)).all()) == [
            "Hello",
            "World",
            "dance",
            "hashtag1",
            "hashtag2",
            "silly",
        ]
        assert session.execute(
            select(Video.id, Hashtag.name).join(Video.hashtags).order_by(Video.id)
        ).all() == [
            (mock_videos[0].id, "hashtag1"),
            (mock_videos[0].id, "hashtag2"),
            (mock_videos[1].id, "hashtag1"),
            (mock_videos[1].id, "hashtag2"),
            (MOCK_VIDEO_DATA["id"], MOCK_VIDEO_DATA["hashtag_names"][0]),
            (MOCK_VIDEO_DATA["id"], MOCK_VIDEO_DATA["hashtag_names"][1]),
        ]


def test_remove_all(in_memory_database, mock_videos, mock_crawl):
    with Session(in_memory_database) as session:
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
