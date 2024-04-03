import datetime

from sqlalchemy import (
    Engine,
    create_engine,
    select,
)
from sqlalchemy.orm import Session
import pytest

from .sql import Crawl, Video, create_tables

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


@pytest.fixture
def in_memory_database() -> Engine:
    engine = create_engine("sqlite://", echo=True)
    create_tables(engine)

    return engine


@pytest.fixture
def mock_videos():
    now = datetime.datetime.now()
    return [
        Video(id=1, username="Testing1", region_code="US", create_time=now),
        Video(
            id=2,
            username="Testing2",
            region_code="US",
            comment_count=1,
            create_time=now,
            effect_ids=[1, 2, 3],
            hashtag_names=["Hello", "World"],
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
        Video.custom_sqlite_upsert(
            [
                {"id": mock_videos[0].id, "share_count": 300},
                {"id": mock_videos[1].id, "share_count": 3},
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


def test_upsert_updates_existing_and_inserts_new_video_data(
    in_memory_database, mock_videos,
):
    with Session(in_memory_database) as session:
        session.add_all(mock_videos)
        session.commit()

        new_source = ["0.0-testing"]
        Video.custom_sqlite_upsert(
            [
                MOCK_VIDEO_DATA,
                {
                    "id": mock_videos[1].id,
                    "comment_count": mock_videos[1].comment_count + 1,
                },
            ],
            source=new_source,
            engine=in_memory_database,
        )
        assert session.execute(
            select(Video.id, Video.comment_count).order_by(Video.id)
        ).all() == [
            (mock_videos[0].id, None),
            (mock_videos[1].id, mock_videos[1].comment_count + 1),
            (MOCK_VIDEO_DATA["id"], MOCK_VIDEO_DATA["comment_count"]),
        ]

        video_in_db = session.scalars(
            select(Video).where(Video.id == MOCK_VIDEO_DATA["id"])
        ).first()
        for k, v in MOCK_VIDEO_DATA.items():
            assert getattr(video_in_db, k) == v


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
