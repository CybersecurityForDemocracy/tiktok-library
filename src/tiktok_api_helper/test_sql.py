import datetime

from sqlalchemy import (
    Engine,
    create_engine,
    select,
)
from sqlalchemy.orm import Session
import pytest

from .sql import Crawl, Video, create_tables

MOCK_VIDEO_DATA = [
    {
        "music_id": 6810180973628491777,
        "playlist_id": 0,
        "region_code": "US",
        "share_count": 50,
        "username": "american_ginger_redeemed",
        "hashtag_names": [
            "whatdidyouexpect",
            "viral",
            "foryou",
            "fyp",
            "prolife",
            "greenscreensticker",
            "unbornlivesmatter",
        ],
        "id": 7094381613995478318,
        "like_count": 2135,
        "view_count": 20777,
        "video_description": "Pregnancy is a natural outcome to unprotected s*x… what did you think was gonna happen? #fyp #foryou #unbornlivesmatter #viral #prolife #whatdidyouexpect #greenscreensticker",
        "comment_count": 501,
        "create_time": 1651789438,
        "effect_ids": ["0"],
    },
    {
        "video_description": "Period. #abortionismurder #fyp #prolife #LaurelRoad4Nurses #BBPlayDate #roemustgo",
        "create_time": 1651766906,
        "effect_ids": ["0"],
        "id": 7094284837128817962,
        "region_code": "US",
        "share_count": 5,
        "view_count": 5400,
        "comment_count": 72,
        "hashtag_names": [
            "fyp",
            "prolife",
            "abortionismurder",
            "LaurelRoad4Nurses",
            "BBPlayDate",
            "roemustgo",
        ],
        "like_count": 499,
        "music_id": 6865506085311088641,
        "username": "realmorganfaith",
    },
    {
        "like_count": 760,
        "music_id": 6833934234948732941,
        "username": "edenmccourt",
        "video_description": "I don’t usually talk about myself on my public pages, but I think given the current climate it is necessary. I want to help you understand that people on both sides of this debate are just normal people with normal interests and who should be treated with respect, dignity and kindness. We can disagree and still be friends. Less polarisation and more conversation. ❤️ #foryourpage #humanlikeyou",
        "view_count": 19365,
        "comment_count": 373,
        "effect_ids": ["0"],
        "id": 7094037673978973446,
        "region_code": "GB",
        "share_count": 30,
        "create_time": 1651709360,
        "hashtag_names": ["humanlikeyou", "foryourpage"],
    },
    {
        "comment_count": 402,
        "create_time": 1651614306,
        "id": 7093629419205561606,
        "like_count": 923,
        "region_code": "GB",
        "username": "edenmccourt",
        "video_description": "It do be like that tho. #fyp #roevwade #abortion",
        "view_count": 13809,
        "effect_ids": ["0"],
        "hashtag_names": ["abortion", "fyp", "roevwade"],
        "music_id": 7016913596630207238,
        "share_count": 16,
    },
]


@pytest.fixture
def in_memory_database() -> Engine:
    engine = create_engine("sqlite:///", echo=True)
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
