import copy
import datetime
import logging
from typing import Any, Optional, List, Mapping
import json
from pathlib import Path
import itertools

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Engine,
    String,
    TypeDecorator,
    create_engine,
    func,
    BigInteger,
    Table,
    ForeignKey,
    UniqueConstraint,
    select,
    SQLColumnExpression,
)
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    synonym,
    relationship,
)
from sqlalchemy.dialects.postgresql import aggregate_order_by

from .query import Query, QueryJSONEncoder


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

        if not isinstance(value, dict):
            raise ValueError("value must be a dict!")

        return value.get("list", [])


class Base(DeclarativeBase):
    pass


video_hashtag_association_table = Table(
    "videos_to_hashtags",
    Base.metadata,
    Column("video_id", ForeignKey("video.id"), primary_key=True),
    Column("hashtag_id", ForeignKey("hashtag.id"), primary_key=True),
)

video_effect_id_association_table = Table(
    "videos_to_effect_ids",
    Base.metadata,
    Column("video_id", ForeignKey("video.id"), primary_key=True),
    Column("effect_id", ForeignKey("effect.id"), primary_key=True),
)

video_query_tag_association_table = Table(
    "videos_to_query_tags",
    Base.metadata,
    Column("video_id", ForeignKey("video.id"), primary_key=True),
    Column("query_tag_id", ForeignKey("query_tag.id"), primary_key=True),
)

crawl_query_tag_association_table = Table(
    "crawls_to_query_tags",
    Base.metadata,
    Column("crawl_id", ForeignKey("crawl.id"), primary_key=True),
    Column("query_tag_id", ForeignKey("query_tag.id"), primary_key=True),
)


class Hashtag(Base):
    __tablename__ = "hashtag"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String)

    __table_args__ = (UniqueConstraint("name"),)

    def __repr__(self) -> str:
        return f"Hashtag (id={self.id}, name={self.name!r})"

class QueryTag(Base):
    __tablename__ = "query_tag"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String)

    __table_args__ = (UniqueConstraint("name"),)

    def __repr__(self) -> str:
        return f"QueryTag (id={self.id}, name={self.name!r})"

class Effect(Base):
    __tablename__ = "effect"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    effect_id: Mapped[str] = mapped_column(String)

    __table_args__ = (UniqueConstraint("effect_id"),)

    def __repr__(self) -> str:
        return f"Effect (id={self.id}, effect_id={self.effect_id!r})"


class Video(Base):
    __tablename__ = "video"

    id: Mapped[int] = mapped_column(BigInteger, autoincrement=False, primary_key=True)
    video_id = synonym("id")
    item_id = synonym("id")

    create_time: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=False),
    )

    username: Mapped[str]
    region_code: Mapped[str] = mapped_column(String(2))
    video_description: Mapped[Optional[str]]
    music_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    like_count: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    comment_count: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    share_count: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    view_count: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    effects: Mapped[List[Effect]] = relationship(secondary=video_effect_id_association_table)
    hashtags: Mapped[List[Hashtag]] = relationship(
        secondary=video_hashtag_association_table
    )

    playlist_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    voice_to_text: Mapped[Optional[str]]

    # Columns here are not returned by the API, but are added by us
    crawled_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    crawled_updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
    )

    # We use Json here just to have list support in SQLite
    # While postgres has array support, sqlite doesn't and we want to keep it agnositc
    source = mapped_column(MyJsonList, nullable=True)
    query_tags: Mapped[List[QueryTag]] = relationship(secondary=video_query_tag_association_table)
    extra_data = Column(
        MUTABLE_JSON, nullable=True
    )  # For future data I haven't thought of yet

    def __repr__(self) -> str:
        return (f"Video (id={self.id!r}, username={self.username!r}, source={self.source!r}), "
                f"hashtags={self.hashtags!r}, query_tags={self.query_tags!r}")

    @property
    def hashtag_names(self):
        return [hashtag.name for hashtag in self.hashtags]

    @property
    def query_tag_names(self):
        return [query_tag.name for query_tag in self.query_tags]

    @property
    def effect_ids(self):
        return [effect.effect_id for effect in self.effects]


def _get_hashtag_name_to_hashtag_object_map(
    session: Session, video_data: list[dict[str, Any]]
) -> Mapping[str, Hashtag]:
    """Gets hashtag name -> Hashtag object map, pulling existing Hashtag objects from database and
    creating new Hashtag objects for new hashtag names.
    """
    # Get all hashtag names references in this list of videos
    hashtag_names_referenced = set(
        itertools.chain.from_iterable(
            [video.get("hashtag_names", []) for video in video_data]
        )
    )
    # Of all the referenced hashtag names get those which exist in the database
    hashtag_name_to_hashtag = {
        row.name: row
        for row in session.scalars(
            select(Hashtag).where(Hashtag.name.in_(hashtag_names_referenced))
        )
    }
    # Make new hashtag objects for hashtag names not yet in the database
    existing_hashtag_names = set(hashtag_name_to_hashtag.keys())
    new_hashtag_names = hashtag_names_referenced - existing_hashtag_names
    for hashtag_name in new_hashtag_names:
        hashtag_name_to_hashtag[hashtag_name] = Hashtag(name=hashtag_name)
    session.add_all(hashtag_name_to_hashtag.values())
    return hashtag_name_to_hashtag

def _get_query_tag_name_to_query_tag_object_map(
        session: Session, source: Optional[List[str]]
) -> List[QueryTag]:
    """Gets query_tag name -> QueryTag object map, pulling existing QueryTag objects from database and
    creating new QueryTag objects for new query_tag names.
    """
    if not source:
        return []
    # Get all query_tag names references in this list of videos
    query_tag_names_referenced = set(source)
    # Of all the referenced query_tag names get those which exist in the database
    query_tag_name_to_query_tag = {
        row.name: row
        for row in session.scalars(
            select(QueryTag).where(QueryTag.name.in_(query_tag_names_referenced))
        )
    }
    # Make new query_tag objects for query_tag names not yet in the database
    existing_query_tag_names = set(query_tag_name_to_query_tag.keys())
    new_query_tag_names = query_tag_names_referenced - existing_query_tag_names
    for query_tag_name in new_query_tag_names:
        query_tag_name_to_query_tag[query_tag_name] = QueryTag(name=query_tag_name)
    session.add_all(query_tag_name_to_query_tag.values())
    return list(query_tag_name_to_query_tag.values())

def _get_effect_id_to_effect_object_map(
    session: Session, video_data: list[dict[str, Any]]
) -> Mapping[str, Effect]:
    """Gets effect id -> Effect object map, pulling existing Effect objects from database and
    creating new Effect objects for new effect ids.
    """
    # Get all effect ids references in this list of videos
    effect_ids_referenced = set(
        itertools.chain.from_iterable(
            [video.get("effect_ids", []) for video in video_data]
        )
    )
    # Of all the referenced effect ids get those which exist in the database
    effect_id_to_effect = {
        row.effect_id: row
        for row in session.scalars(
            select(Effect).where(Effect.effect_id.in_(effect_ids_referenced))
        )
    }
    # Make new effect objects for effect ids not yet in the database
    existing_effect_ids = set(effect_id_to_effect.keys())
    new_effect_ids = effect_ids_referenced - existing_effect_ids
    for effect_id in new_effect_ids:
        effect_id_to_effect[effect_id] = Effect(effect_id=effect_id)
    session.add_all(effect_id_to_effect.values())
    return effect_id_to_effect


def upsert_videos(
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
    with Session(engine) as session:
        # Get all hashtag names references in this list of videos
        hashtag_name_to_hashtag = _get_hashtag_name_to_hashtag_object_map(
            session, video_data
        )

        # Get all query_tag names references in this list of videos
        query_tags = _get_query_tag_name_to_query_tag_object_map(
            session, source
        )

        # Get all effect ids references in this list of videos
        effect_id_to_effect = _get_effect_id_to_effect_object_map(
            session, video_data
        )

        video_id_to_video = {}

        for vid in video_data:
            # manually add the source, keeping the original dict intact
            new_vid = copy.deepcopy(vid)
            new_vid["source"] = source
            new_vid["create_time"] = datetime.datetime.fromtimestamp(vid["create_time"])
            if "effect_ids" in vid:
                new_vid["effects"] = [effect_id_to_effect[effect_id] for effect_id in vid["effect_ids"]]
                del new_vid["effect_ids"]
            if "hashtag_names" in vid:
                new_vid["hashtags"] = [
                    hashtag_name_to_hashtag[hashtag_name]
                    for hashtag_name in vid["hashtag_names"]
                ]
                del new_vid["hashtag_names"]
            if source:
                new_vid["query_tags"] = query_tags

            video_id_to_video[vid["id"]] = new_vid

        # Taken from https://stackoverflow.com/questions/25955200/sqlalchemy-performing-a-bulk-upsert-if-exists-update-else-insert-in-postgr

        # Merge all the videos that already exist
        for each in session.scalars(
            select(Video).where(Video.id.in_(video_id_to_video.keys()))
        ):
            new_vid = Video(**video_id_to_video.pop(each.id))
            new_vid.source = each.source + new_vid.source
            session.merge(new_vid)

        session.add_all((Video(**vid) for vid in video_id_to_video.values()))

        session.commit()
        session.expire_all()


class Crawl(Base):
    __tablename__ = "crawl"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    crawl_started_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    cursor: Mapped[int] = mapped_column(BigInteger)
    has_more: Mapped[bool]
    search_id: Mapped[str]
    query: Mapped[str]

    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), server_default=func.now()
    )

    source = mapped_column(MyJsonList, nullable=True)
    query_tags: Mapped[List[QueryTag]] = relationship(secondary=crawl_query_tag_association_table)
    extra_data = Column(
        MUTABLE_JSON, nullable=True
    )  # For future data I haven't thought of yet

    def __repr__(self) -> str:
        return (
            f"Query source={self.source!r}, query_tags={self.query_tags!r}, "
            f"started_at={self.crawl_started_at!r}, "
            f"has_more={self.has_more!r}, search_id={self.search_id!r}\n"
            f"query='{self.query!r}'"
        )

    @classmethod
    def from_request(
        cls, res_data: dict, query: Query, source: Optional[list[str]] = None
    ) -> "Crawl":
        return cls(
            cursor=res_data["cursor"],
            has_more=res_data["has_more"],
            search_id=res_data["search_id"],
            query=json.dumps(query, cls=QueryJSONEncoder),
            source=source,
        )

    def upload_self_to_db(self, engine: Engine) -> None:
        """Uploads current instance to DB"""
        with Session(engine) as session:
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


def get_sqlite_engine_and_create_tables(db_path: Path, **kwargs) -> Engine:
    return get_engine_and_create_tables(f"sqlite:///{db_path.absolute()}", **kwargs)


def get_engine_and_create_tables(db_url: str, **kwargs) -> Engine:
    engine = create_engine(db_url, **kwargs)
    create_tables(engine)

    return engine


def create_tables(engine: Engine) -> None:
    Base.metadata.create_all(engine, checkfirst=True)


def convert_to_json(lst_: list) -> dict:
    """For storing lists in SQLite, we need to convert them to JSON"""
    if not isinstance(lst_, list):
        raise ValueError("lst_ must be a list!")

    return {"list": lst_}
