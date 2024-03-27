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

    @classmethod
    def from_request(
        cls, res_data: dict, query, source: Optional[list[str]] = None
    ) -> "Crawl":
        return cls(
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
