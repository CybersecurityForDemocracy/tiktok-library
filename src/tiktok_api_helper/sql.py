import copy
import datetime
import logging
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Engine,
    ForeignKey,
    String,
    TypeDecorator,
    create_engine,
    func,
    select,
)
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, synonym

from .custom_types import DBFileType

TEST_DB_PATH = Path("./test.db")

# See https://amercader.net/blog/beware-of-json-fields-in-sqlalchemy/
MUTABLE_JSON = MutableDict.as_mutable(JSON)  # type: ignore


class MyJsonList(TypeDecorator):
    """We override the JSON type to natively support lists better.
    This is because SQLite doesn't support lists natively, so we need to convert them to JSON
    """

    impl = MUTABLE_JSON

    cache_ok = True

    def coerce_compared_value(self, op, value):
        """Needed - See the warning section in the docs
        https://docs.sqlalchemy.org/en/20/core/custom_types.html
        """
        return self.impl.coerce_compared_value(op, value)  # type: ignore

    def process_bind_param(
        self, value: Optional[list], dialect
    ) -> dict[str, list] | None:
        if value is None:
            return None

        return convert_to_json(value)

    def process_result_value(self, value: Optional[dict[str, list]], dialect) -> list:
        if value is None:
            return []

        elif not isinstance(value, dict):
            raise ValueError("value must be a dict!")

        return value.get("list", [])


class Base(DeclarativeBase):
    pass


class Video(Base):
    __tablename__ = "video"

    id: Mapped[int] = mapped_column(primary_key=True)
    video_id = synonym("id")
    item_id = synonym("id")

    create_time: Mapped[int]

    username: Mapped[str]
    region_code: Mapped[str] = mapped_column(String(2))
    video_description: Mapped[Optional[str]]
    music_id: Mapped[Optional[int]]

    like_count: Mapped[Optional[int]]
    comment_count: Mapped[Optional[int]]
    share_count: Mapped[Optional[int]]
    view_count: Mapped[Optional[int]]

    # We use Json here just to have list support in SQLite
    # While postgres has array support, sqlite doesn't and we want to keep it agnositc
    effect_ids = mapped_column(MyJsonList, nullable=True)
    hashtag_names = mapped_column(MyJsonList, nullable=True)

    playlist_id: Mapped[Optional[int]]
    voice_to_text: Mapped[Optional[str]]

    # Columns here are not returned by the API, but are added by us
    crawled_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    crawled_updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
    )

    source = mapped_column(MyJsonList, nullable=True)
    extra_data = Column(
        MUTABLE_JSON, nullable=True
    )  # For future data I haven't thought of yet

    def __repr__(self) -> str:
        return f"Video (id={self.id!r}, username={self.username!r}, source={self.source!r})"

    @staticmethod
    def custom_sqlite_upsert(
        video_data: list[dict[str, Any]],
        engine: Engine,
        source: Optional[list[str]] = None,
    ):
        """
        Columns must be the same when doing a upsert which is annoying since we have
            different rows w/ different cols - Instead we just do a custom upsert

        I.e.
            stmt = sqlite_upsert(Video).values(videos)
            stmt = stmt.on_conflict_do_update(...)
        does not work
        """
        ids_to_video = {}

        for vid in video_data:
            # manually add the source, keeping the original dict intact
            new_vid = copy.deepcopy(vid)
            new_vid["source"] = source

            ids_to_video[vid["id"]] = new_vid

        # Taken from https://stackoverflow.com/questions/25955200/sqlalchemy-performing-a-bulk-upsert-if-exists-update-else-insert-in-postgr
        with Session(engine) as session:

            # Merge all the videos that already exist
            for each in session.query(Video).filter(Video.id.in_(ids_to_video.keys())):
                new_vid = Video(**ids_to_video.pop(each.id))
                new_vid.source = each.source + new_vid.source
                session.merge(new_vid)

            session.add_all((Video(**vid) for vid in ids_to_video.values()))

            session.commit()


class Crawl(Base):
    __tablename__ = "Crawl"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    crawl_started_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    cursor: Mapped[int]
    has_more: Mapped[bool]
    search_id: Mapped[str]
    query: Mapped[str]

    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), server_default=func.now()
    )

    source = mapped_column(MyJsonList, nullable=True)
    extra_data = Column(
        MUTABLE_JSON, nullable=True
    )  # For future data I haven't thought of yet

    def __repr__(self) -> str:
        return (
            f"Query source={self.source!r}, started_at={self.crawl_started_at!r},"
            f"has_more={self.has_more!r}, search_id={self.search_id!r}\n"
            f"query='{self.query!r}'"
        )

    @staticmethod
    def from_request(
        res_data: dict, query, source: Optional[list[str]] = None
    ) -> "Crawl":
        return Crawl(
            cursor=res_data["cursor"],
            has_more=res_data["has_more"],
            search_id=res_data["search_id"],
            query=str(query),
            source=source,
        )

    def upload_self_to_db(self, engine: Engine) -> None:
        """Uploads current instance to DB"""
        with get_sql_session(engine) as session:
            # Reconcile self with an instance of the same primary key in the session.
            # Otherwise loads the object from the database based on primary key,
            #   and if none can be located, creates a new instance.
            session.merge(self)
            session.commit()

    def update_crawl(self, next_res_data: dict, videos: list[str], engine: Engine):
        self.cursor = next_res_data["cursor"]
        self.has_more = next_res_data["has_more"]

        if next_res_data["search_id"] != self.search_id:
            logging.log(
                logging.ERROR,
                f"search_id changed! Was {self.search_id} now {next_res_data['search_id']}",
            )
            self.search_id = next_res_data["search_id"]

        self.updated_at = datetime.datetime.now()

        # Update the number of videos that were possibly deleted
        if self.extra_data is None:
            current_deleted_count = 0
        else:
            current_deleted_count = self.extra_data.get("possibly_deleted", 0)

        n_videos = len(videos)

        # assumes we're using the maximum 100 videos per request
        self.extra_data = {"possibly_deleted": (100 - n_videos) + current_deleted_count}

        self.upload_self_to_db(engine)


def get_engine_and_create_tables(db_path: DBFileType, **kwargs) -> Engine:
    engine = create_engine(f"sqlite:///{db_path}", **kwargs)
    create_tables(engine)

    return engine


def get_sql_session(engine: Engine) -> Session:
    return Session(engine)


def create_tables(engine: Engine) -> None:
    Base.metadata.create_all(engine, checkfirst=True)


def convert_to_json(lst_: list) -> dict:
    """For storing lists in SQLite, we need to convert them to JSON"""
    if not isinstance(lst_, list):
        raise ValueError("lst_ must be a list!")

    return {"list": lst_}


# TODO: Move this to it's testing configuration


def test_upsert():
    videos = [
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

    Video.custom_sqlite_upsert(
        videos,
        source=[
            "0.0-testing",
        ],
        engine=get_engine_and_create_tables(TEST_DB_PATH, echo=True),
    )


def test_print_all():
    engine = get_engine_and_create_tables(TEST_DB_PATH, echo=True)

    with Session(engine) as session:
        for video in session.scalars(select(Video)):
            print(video)

        print("Videos done")

        for crawl in session.scalars(select(Crawl)):
            print(crawl)

        print("Data inspection done\n\n")


def test_remove_all():
    engine = get_engine_and_create_tables(TEST_DB_PATH, echo=True)

    with Session(engine) as session:
        for video in session.scalars(select(Video)):
            print("Deleting video", video)
            session.delete(video)

        for crawl in session.scalars(select(Crawl)):
            print("Deleting crawl", crawl)
            session.delete(crawl)

        session.commit()


def test_creation():
    engine = get_engine_and_create_tables(TEST_DB_PATH)
    now = datetime.datetime.now()

    with Session(engine) as session:
        testing1 = Video(id=1, username="Testing1", region_code="US", create_time=now)
        testing2 = Video(
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
        )

        session.add_all([testing1, testing2])
        session.commit()

        testing3 = Crawl(
            cursor=1, has_more=False, search_id="test", query="test", source=["testing"]
        )
        session.add_all([testing3])
        session.commit()

    test_print_all()
    test_remove_all()
    test_upsert()
    test_remove_all()

    print("All tests done")
