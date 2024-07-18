import datetime
import itertools

import pytest
from sqlalchemy import (
    select,
)
from sqlalchemy.orm import Session

from tests.common import (
    all_crawls,
    all_hashtag_names_sorted,
    all_hashtags,
    all_videos,
)
from tiktok_research_api_helper.models import (
    Crawl,
    CrawlTag,
    Effect,
    Hashtag,
    Video,
    upsert_videos,
)


@pytest.fixture
def mock_crawl_tags():
    return {CrawlTag(name="testing")}


@pytest.fixture
def mock_crawl(mock_crawl_tags):
    return Crawl(
        cursor=1,
        has_more=False,
        search_id="test",
        query="test",
        crawl_tags=mock_crawl_tags,
    )


@pytest.fixture
def mock_videos(mock_crawl):
    now = datetime.datetime.now(tz=datetime.UTC)
    return [
        Video(
            id=1,
            username="Testing1",
            region_code="US",
            create_time=now,
            hashtags={Hashtag(name="hashtag1"), Hashtag(name="hashtag2")},
            crawls={mock_crawl},
        ),
        Video(
            id=2,
            username="Testing2",
            region_code="US",
            comment_count=1,
            create_time=now,
            effects={
                Effect(effect_id=101),
                Effect(effect_id=202),
                Effect(effect_id=303),
            },
            hashtags={Hashtag(name="Hello"), Hashtag(name="World")},
            playlist_id=7044254287731739397,
            voice_to_text="a string",
            extra_data={"some-future-field-i-havent-thought-of": ["value"]},
            crawl_tags=mock_crawl.crawl_tags,
            crawls={mock_crawl},
        ),
    ]


@pytest.fixture
def api_response_videos(testdata_api_videos_response_json):
    return testdata_api_videos_response_json["data"]["videos"]


def assert_video_database_object_list_matches_api_responses_dict(
    video_objects, api_responses_video_dict
):
    video_id_to_database_object = {video.id: video for video in video_objects}
    video_id_to_api_response_dict = {
        api_response_dict["id"]: api_response_dict for api_response_dict in api_responses_video_dict
    }
    database_video_ids = set(video_id_to_database_object.keys())
    api_responses_video_ids = set(video_id_to_api_response_dict.keys())
    assert database_video_ids == api_responses_video_ids, (
        f"Database objects missing IDs in API response "
        f"({api_responses_video_ids - database_video_ids}). API responses missing IDs in database "
        f"objects ({database_video_ids - api_responses_video_ids})"
    )
    for video_id in database_video_ids:
        _assert_video_database_object_matches_api_response_dict(
            video_id_to_database_object[video_id],
            video_id_to_api_response_dict[video_id],
        )


def _assert_video_database_object_matches_api_response_dict(video_object, api_response_video_dict):
    for k, v in api_response_video_dict.items():
        try:
            db_value = getattr(video_object, k)
            if isinstance(db_value, datetime.datetime):
                db_value = db_value.timestamp()
            if isinstance(db_value, set):
                db_value = list(db_value)
            if isinstance(db_value, list):
                db_value.sort()
                v.sort()

            assert db_value == v, (
                f"Video object {video_object!r} attribute {k} value {getattr(video_object, k)} != "
                f"API response dict value {v}; full API response dict:\n{api_response_video_dict}"
            )

        except AttributeError as e:
            error_msg = f"Video object {video_object!r} has not attribute {k}: {e}"
            raise ValueError(error_msg) from e


def test_video_basic_insert(test_database_engine, mock_videos):
    with Session(test_database_engine) as session:
        session.add_all(mock_videos)
        session.commit()
        assert all_videos(session) == mock_videos


def test_crawl_basic_insert(test_database_engine):
    with Session(test_database_engine) as session:
        mock_crawl = Crawl(
            id=1,
            cursor=1,
            has_more=False,
            search_id="test",
            query="test",
            crawl_tags={CrawlTag(name="testing")},
        )
        session.add_all(mock_crawl.crawl_tags)
        session.add_all([mock_crawl])
        session.commit()

        assert all_crawls(session) == [mock_crawl]


def test_crawl_tags_inserted_via_crawl(test_database_engine, mock_crawl):
    assert mock_crawl.crawl_tags
    for crawl_tag in mock_crawl.crawl_tags:
        assert crawl_tag.name
        assert not crawl_tag.id

    crawl_tag_names = {crawl_tag.name for crawl_tag in mock_crawl.crawl_tags}

    mock_crawl.upload_self_to_db(test_database_engine)
    assert mock_crawl.id is not None
    initial_mock_crawl_id = mock_crawl.id

    with Session(test_database_engine) as session:
        assert {
            crawl_tag.name
            for crawl_tag in session.scalar(
                select(Crawl).where(Crawl.id == mock_crawl.id)
            ).crawl_tags
        } == crawl_tag_names

    # Confirm uploading to database again does not cause issue.
    mock_crawl.upload_self_to_db(test_database_engine)
    assert mock_crawl.id == initial_mock_crawl_id

    # Now add some tags
    more_crawl_tag_names = crawl_tag_names | {"crawl_tag1", "crawl_tag2"}
    new_crawl = Crawl.from_request(
        res_data={"cursor": mock_crawl.cursor, "has_more": True, "search_id": 1},
        query="{}",
        crawl_tags=more_crawl_tag_names,
    )
    assert new_crawl.id is None
    new_crawl.upload_self_to_db(test_database_engine)
    assert new_crawl.id is not None
    # Want to make sure Crawl.from_request gets a new ID
    assert new_crawl.id != initial_mock_crawl_id
    with Session(test_database_engine) as session:
        assert {
            crawl_tag.name
            for crawl_tag in session.scalar(
                select(Crawl).where(Crawl.id == new_crawl.id)
            ).crawl_tags
        } == more_crawl_tag_names


def test_none_crawl_tags(test_database_engine, mock_crawl):
    initial_mock_crawl_id = mock_crawl.id
    crawl = Crawl.from_request(
        res_data={"cursor": mock_crawl.cursor, "has_more": True, "search_id": 1},
        query="{}",
        crawl_tags=None,
    )
    crawl.upload_self_to_db(test_database_engine)
    assert crawl.id is not None
    # Want to make sure Crawl.from_request gets a new ID
    assert crawl.id != initial_mock_crawl_id
    with Session(test_database_engine) as session:
        assert {
            crawl_tag.name
            for crawl_tag in session.scalar(select(Crawl).where(Crawl.id == crawl.id)).crawl_tags
        } == set()


def test_upsert(test_database_engine, mock_videos, mock_crawl):
    with Session(test_database_engine) as session:
        session.add_all(mock_videos)
        session.commit()
        assert {v.id: {*v.crawl_tag_names} for v in all_videos(session)} == {
            mock_videos[0].id: {*mock_videos[0].crawl_tag_names},
            mock_videos[1].id: {*mock_videos[1].crawl_tag_names},
        }
        assert session.scalars(select(Video.share_count).order_by(Video.id)).all() == [
            None,
            None,
        ]

        new_crawl_tags = ["testing", "0.0-testing"]
        upsert_videos(
            [
                {
                    "id": mock_videos[0].id,
                    "share_count": 300,
                    "create_time": datetime.datetime.now(datetime.UTC).timestamp(),
                    "hashtag_names": ["hashtag1", "hashtag2"],
                },
                {
                    "id": mock_videos[1].id,
                    "share_count": 3,
                    "create_time": datetime.datetime.now(datetime.UTC).timestamp(),
                    "hashtag_names": ["hashtag1", "hashtag2", "hashtag3"],
                },
            ],
            crawl_id=mock_crawl.id,
            crawl_tags=new_crawl_tags,
            engine=test_database_engine,
        )
        session.expire_all()
        assert {v.id: {*v.crawl_tag_names} for v in all_videos(session)} == {
            mock_videos[0].id: {"testing", "0.0-testing"},
            mock_videos[1].id: {"testing", "0.0-testing"},
        }
        assert session.scalars(select(Video.share_count).order_by(Video.id)).all() == [
            300,
            3,
        ]
        assert {
            v.id: {hashtag.name for hashtag in v.hashtags}
            for v in session.scalars(select(Video).join(Video.hashtags).order_by(Video.id)).all()
        } == {
            mock_videos[0].id: {"hashtag1", "hashtag2"},
            mock_videos[1].id: {"hashtag1", "hashtag2", "hashtag3"},
        }


def test_upsert_existing_video_and_new_video_upserted_together(
    test_database_engine, mock_videos, mock_crawl
):
    with Session(test_database_engine) as session:
        #  session.add_all(mock_crawl.crawl_tags)
        #  session.add_all([mock_crawl])
        session.add_all([mock_videos[0]])
        session.commit()
        assert {v.id: {*v.crawl_tag_names} for v in all_videos(session)} == {
            mock_videos[0].id: set(mock_videos[0].crawl_tags),
        }
        assert session.scalars(select(Video.share_count).order_by(Video.id)).all() == [
            None,
        ]

        new_crawl_tags = ["testing", "0.0-testing"]
        upsert_videos(
            [
                {
                    "id": mock_videos[0].id,
                    "share_count": 300,
                    "create_time": datetime.datetime.now(datetime.UTC).timestamp(),
                    "hashtag_names": ["hashtag1", "hashtag2"],
                },
                {
                    "id": mock_videos[1].id,
                    "share_count": mock_videos[1].share_count,
                    "create_time": datetime.datetime.now(datetime.UTC).timestamp(),
                    "hashtag_names": mock_videos[1].hashtag_names,
                    "region_code": mock_videos[1].region_code,
                    "username": mock_videos[1].username,
                },
            ],
            crawl_id=mock_crawl.id,
            crawl_tags=new_crawl_tags,
            engine=test_database_engine,
        )
        session.expire_all()
        assert {v.id: {*v.crawl_tag_names} for v in all_videos(session)} == {
            mock_videos[0].id: set(new_crawl_tags),
            mock_videos[1].id: set(new_crawl_tags),
        }
        assert session.scalars(select(Video.share_count).order_by(Video.id)).all() == [
            300,
            mock_videos[1].share_count,
        ]
        assert {v.id: {*v.hashtag_names} for v in all_videos(session)} == {
            mock_videos[0].id: {"hashtag1", "hashtag2"},
            mock_videos[1].id: {"Hello", "World"},
        }


def test_upsert_no_prior_insert(test_database_engine, mock_videos, mock_crawl):
    new_crawl_tags = ["0.0-testing"]
    upsert_videos(
        [
            {
                "id": mock_videos[0].id,
                "username": "tron",
                "region_code": "US",
                "share_count": 300,
                "create_time": datetime.datetime.now(datetime.UTC).timestamp(),
                "hashtag_names": ["hashtag1", "hashtag2"],
            },
            {
                "id": mock_videos[1].id,
                "username": "tron",
                "region_code": "US",
                "share_count": 3,
                "create_time": datetime.datetime.now(datetime.UTC).timestamp(),
                "hashtag_names": ["hashtag1", "hashtag2"],
            },
        ],
        crawl_id=mock_crawl.id,
        crawl_tags=new_crawl_tags,
        engine=test_database_engine,
    )
    with Session(test_database_engine) as session:
        assert {v.id: {*v.crawl_tag_names} for v in all_videos(session)} == {
            mock_videos[0].id: set(new_crawl_tags),
            mock_videos[1].id: set(new_crawl_tags),
        }
        assert session.scalars(select(Video.share_count).order_by(Video.id)).all() == [
            300,
            3,
        ]
        assert [{*v.hashtag_names} for v in all_videos(session)] == [
            {"hashtag1", "hashtag2"},
            {"hashtag1", "hashtag2"},
        ]


def test_upsert_videos_to_crawls_association(test_database_engine, mock_crawl, api_response_videos):
    with Session(test_database_engine) as session:
        mock_crawl.upload_self_to_db(test_database_engine)
        expected_crawl_id = mock_crawl.id

    upsert_videos(
        api_response_videos,
        crawl_id=mock_crawl.id,
        crawl_tags=["testing"],
        engine=test_database_engine,
    )
    with Session(test_database_engine) as session:
        assert {v.id: {crawl.id for crawl in v.crawls} for v in all_videos(session)} == {
            v["id"]: {expected_crawl_id} for v in api_response_videos
        }


def test_upsert_existing_hashtags_names_gets_same_id(
    test_database_engine,
    mock_crawl,
):
    """Tests that adding a video with an existing hashtag name (from a previously added video)
    succeeds, gets the same ID it had previously, and does not raise a Unique violation error.
    """
    utcnow = datetime.datetime.now(datetime.UTC).timestamp()
    with Session(test_database_engine) as session:
        session.add_all(mock_crawl.crawl_tags)
        session.add_all([mock_crawl])
        session.commit()

        new_crawl_tags = ["0.0-testing"]
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
            crawl_id=mock_crawl.id,
            crawl_tags=new_crawl_tags,
            engine=test_database_engine,
        )

        original_hashtags = {hashtag.id: hashtag.name for hashtag in all_hashtags(session)}
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
            crawl_id=mock_crawl.id,
            crawl_tags=new_crawl_tags,
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
        assert [(v.id, {*v.hashtag_names}) for v in all_videos(session)] == [
            (0, {"hashtag1", "hashtag2"}),
            (1, {"hashtag1", "hashtag2", "hashtag3"}),
        ]


def test_upsert_updates_existing_and_inserts_new_video_data(
    test_database_engine,
    mock_videos,
    mock_crawl,
    api_response_videos,
):
    utcnow = datetime.datetime.now(datetime.UTC).timestamp()
    with Session(test_database_engine) as session:
        session.add_all(mock_crawl.crawl_tags)
        session.add_all([mock_crawl])
        session.add_all(mock_videos)
        session.commit()

        new_crawl_tags = ["0.0-testing"]
        upsert_videos(
            [
                api_response_videos[0],
                {
                    "id": mock_videos[1].id,
                    "comment_count": 200,
                    "create_time": utcnow,
                    "hashtag_names": ["hashtag1", "hashtag2"],
                },
            ],
            crawl_id=mock_crawl.id,
            crawl_tags=new_crawl_tags,
            engine=test_database_engine,
        )
        session.expire_all()
        assert session.execute(
            select(Video.id, Video.comment_count, Video.create_time).order_by(Video.id)
        ).all() == [
            (mock_videos[0].id, None, mock_videos[0].create_time),
            (
                mock_videos[1].id,
                200,
                datetime.datetime.fromtimestamp(utcnow, tz=None),
            ),
            (
                api_response_videos[0]["id"],
                api_response_videos[0]["comment_count"],
                datetime.datetime.fromtimestamp(api_response_videos[0]["create_time"], tz=None),
            ),
        ]


def test_upsert_updates_existing_and_inserts_new_video_data_and_hashtag_names(
    test_database_engine,
    mock_videos,
    mock_crawl,
    api_response_videos,
):
    utcnow = datetime.datetime.now(datetime.UTC).timestamp()
    with Session(test_database_engine) as session:
        session.add_all(mock_crawl.crawl_tags)
        session.add_all([mock_crawl])
        session.add_all(mock_videos)
        session.commit()

        new_crawl_tags = ["0.0-testing"]
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
            crawl_id=mock_crawl.id,
            crawl_tags=new_crawl_tags,
            engine=test_database_engine,
        )
        session.expire_all()

        assert all_hashtag_names_sorted(session) == [
            "Hello",
            "World",
            "cats",
            "duet",
            "hashtag1",
            "hashtag2",
        ]

        assert [(v.id, {*v.hashtag_names}) for v in all_videos(session)] == [
            (mock_videos[0].id, {"hashtag1", "hashtag2"}),
            (mock_videos[1].id, {"hashtag1", "hashtag2"}),
            (api_response_videos[0]["id"], {*api_response_videos[0]["hashtag_names"]}),
        ]

        # Now add a new, and remove a previous, hashtag name from mock_videos[1]
        upsert_videos(
            [
                {
                    "id": mock_videos[1].id,
                    "create_time": utcnow,
                    "hashtag_names": ["hashtag2", "hashtag3"],
                },
            ],
            crawl_id=mock_crawl.id,
            crawl_tags=new_crawl_tags,
            engine=test_database_engine,
        )
        session.expire_all()
        assert all_hashtag_names_sorted(session) == [
            "Hello",
            "World",
            "cats",
            "duet",
            "hashtag1",
            "hashtag2",
            "hashtag3",
        ]

        session.expire_all()

        assert [
            (v.id, {*v.hashtag_names})
            for v in session.scalars(
                select(Video).where(Video.id == mock_videos[1].id).order_by(Video.id)
            ).all()
        ] == [
            (mock_videos[1].id, {"hashtag2", "hashtag3"}),
        ]


def test_upsert_updates_existing_and_inserts_new_video_data_and_effect_id(
    test_database_engine,
    mock_videos,
    mock_crawl,
    api_response_videos,
):
    # This video has effect_ids
    api_response_video = api_response_videos[19]
    utcnow = datetime.datetime.now(datetime.UTC).timestamp()
    with Session(test_database_engine) as session:
        session.add_all(mock_crawl.crawl_tags)
        session.add_all([mock_crawl])
        session.add_all(mock_videos)
        session.commit()

        new_crawl_tags = ["0.0-testing"]
        upsert_videos(
            [
                api_response_video,
                {
                    "id": mock_videos[1].id,
                    "comment_count": mock_videos[1].comment_count + 1,
                    "create_time": utcnow,
                    # duplicate effect ID in list intentionally used since API does this sometimes
                    "effect_ids": ["101", "202", "303", "404", "404"],
                },
            ],
            crawl_id=mock_crawl.id,
            crawl_tags=new_crawl_tags,
            engine=test_database_engine,
        )
        session.expire_all()

        assert sorted(session.scalars(select(Effect.effect_id)).all()) == [
            "101",
            "202",
            "303",
            "404",
            "63960564",
        ]

        assert {v.id: v.effect_ids for v in all_videos(session)} == {
            mock_videos[0].id: set(),
            mock_videos[1].id: {"101", "202", "303", "404"},
            api_response_video["id"]: set(api_response_video["effect_ids"]),
        }


def test_upsert_updates_existing_and_inserts_new_video_data_and_crawl_tags(
    test_database_engine,
    mock_videos,
    mock_crawl,
    api_response_videos,
):
    # Test adding crawl_tags from an API response
    api_response_video = api_response_videos[0]
    utcnow = datetime.datetime.now(datetime.UTC).timestamp()
    #  mock_crawl.upload_self_to_db(test_database_engine)
    with Session(test_database_engine) as session:
        session.add_all(mock_crawl.crawl_tags)
        session.add(mock_crawl)
        session.add_all(mock_videos)
        session.commit()

        original_crawl_tags = {v.id: v.crawl_tag_names for v in all_videos(session)}
        new_crawl_tags = {"0.0-testing"}
        upsert_videos(
            [
                api_response_video,
                {
                    "id": mock_videos[1].id,
                    "comment_count": mock_videos[1].comment_count + 1,
                    "create_time": utcnow,
                },
            ],
            crawl_id=mock_crawl.id,
            crawl_tags=new_crawl_tags,
            engine=test_database_engine,
        )
        session.expire_all()
        expected_crawl_tags = (
            set(itertools.chain.from_iterable(original_crawl_tags.values())) | new_crawl_tags
        )

        assert set(session.scalars(select(CrawlTag.name)).all()) == expected_crawl_tags

        assert {v.id: v.crawl_tag_names for v in all_videos(session)} == {
            mock_videos[0].id: original_crawl_tags[mock_videos[0].id],
            mock_videos[1].id: original_crawl_tags[mock_videos[1].id] | new_crawl_tags,
            api_response_video["id"]: new_crawl_tags,
        }


def test_upsert_api_response_videos(test_database_engine, mock_crawl, api_response_videos):
    with Session(test_database_engine) as session:
        session.add_all(mock_crawl.crawl_tags)
        session.add_all([mock_crawl])
        upsert_videos(api_response_videos, crawl_id=mock_crawl.id, engine=test_database_engine)
        session.expire_all()
        assert_video_database_object_list_matches_api_responses_dict(
            session.scalars(select(Video)).all(), api_response_videos
        )


def test_remove_all(test_database_engine, mock_videos, mock_crawl):
    with Session(test_database_engine) as session:
        session.add_all(mock_crawl.crawl_tags)
        session.add_all([mock_crawl])
        session.add_all(mock_videos)
        session.commit()
        assert all_videos(session) == mock_videos
        assert all_crawls(session) == [mock_crawl]

        for video in session.scalars(select(Video)):
            session.delete(video)

        for crawl in session.scalars(select(Crawl)):
            session.delete(crawl)

        session.commit()
        assert all_videos(session) == []
        assert all_crawls(session) == []
