import copy
import datetime
import itertools
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Engine,
    ForeignKey,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    create_engine,
    func,
    select,
)
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    synonym,
)

from tiktok_research_api_helper.query import VideoQuery, VideoQueryJSONEncoder

# See https://amercader.net/blog/beware-of-json-fields-in-sqlalchemy/
MUTABLE_JSON = MutableDict.as_mutable(JSON)  # type: ignore


# Copied from https://stackoverflow.com/a/23175518
# SQLAlchemy does not map BigInt to Int by default on the sqlite dialect (even though " a column
# with type INTEGER PRIMARY KEY is an alias for the ROWID (except in WITHOUT ROWID tables) which is
# always a 64-bit signed integer." according to sqlite documentation
# https://www.sqlite.org/autoinc.html#summary. Thus primrary keys of this type do not get assigned
# an autoincrement default new value correctly.
BigIntegerForPrimaryKeyType = BigInteger()
BigIntegerForPrimaryKeyType = BigIntegerForPrimaryKeyType.with_variant(
    postgresql.BIGINT(), "postgresql"
)
BigIntegerForPrimaryKeyType = BigIntegerForPrimaryKeyType.with_variant(sqlite.INTEGER(), "sqlite")


class Base(DeclarativeBase):
    metadata = MetaData(
        naming_convention={
            "ix": "%(column_0_label)s_idx",
            "uq": "%(table_name)s_%(column_0_name)s_uniq",
            "ck": "%(table_name)s_%(constraint_name)s_check",
            "fk": "%(table_name)s_%(column_0_name)s_%(referred_table_name)s_fkey",
            "pk": "%(table_name)s_pkey",
        }
    )


videos_to_hashtags_association_table = Table(
    "videos_to_hashtags",
    Base.metadata,
    Column("video_id", ForeignKey("video.id"), primary_key=True),
    Column("hashtag_id", ForeignKey("hashtag.id"), primary_key=True),
)


class Hashtag(Base):
    __tablename__ = "hashtag"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String)

    __table_args__ = (UniqueConstraint("name"),)

    def __repr__(self) -> str:
        return f"Hashtag (id={self.id}, name={self.name!r})"


videos_to_crawl_tags_association_table = Table(
    "videos_to_crawl_tags",
    Base.metadata,
    Column("video_id", ForeignKey("video.id"), primary_key=True),
    Column("crawl_tag_id", ForeignKey("crawl_tag.id"), primary_key=True),
)


crawls_to_crawl_tags_association_table = Table(
    "crawls_to_crawl_tags",
    Base.metadata,
    Column("crawl_id", ForeignKey("crawl.id"), primary_key=True),
    Column("crawl_tag_id", ForeignKey("crawl_tag.id"), primary_key=True),
)


class CrawlTag(Base):
    __tablename__ = "crawl_tag"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String)

    __table_args__ = (UniqueConstraint("name"),)

    def __repr__(self) -> str:
        return f"CrawlTag (id={self.id}, name={self.name!r})"


videos_to_effect_ids_association_table = Table(
    "videos_to_effect_ids",
    Base.metadata,
    Column("video_id", ForeignKey("video.id"), primary_key=True),
    Column("effect_id", ForeignKey("effect.id"), primary_key=True),
)


class Effect(Base):
    __tablename__ = "effect"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    effect_id: Mapped[str] = mapped_column(String)

    __table_args__ = (UniqueConstraint("effect_id"),)

    def __repr__(self) -> str:
        return f"Effect (id={self.id}, effect_id={self.effect_id!r})"


videos_to_crawls_association_table = Table(
    "videos_to_crawls",
    Base.metadata,
    Column("video_id", ForeignKey("video.id"), primary_key=True),
    Column("crawl_id", ForeignKey("crawl.id"), primary_key=True),
)


class Video(Base):
    __tablename__ = "video"

    id: Mapped[int] = mapped_column(
        BigIntegerForPrimaryKeyType, autoincrement=False, primary_key=True
    )
    crawls: Mapped[set["Crawl"]] = relationship(secondary=videos_to_crawls_association_table)
    video_id = synonym("id")
    item_id = synonym("id")

    create_time: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=False))

    username: Mapped[str]
    region_code: Mapped[str] = mapped_column(String(2))
    video_description: Mapped[str | None]
    music_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    like_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    comment_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    share_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    view_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    effects: Mapped[set[Effect]] = relationship(secondary=videos_to_effect_ids_association_table)
    hashtags: Mapped[set[Hashtag]] = relationship(secondary=videos_to_hashtags_association_table)

    playlist_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    voice_to_text: Mapped[str | None]

    # Columns here are not returned by the API, but are added by us
    crawled_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    crawled_updated_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
    )

    crawl_tags: Mapped[set[CrawlTag]] = relationship(
        secondary=videos_to_crawl_tags_association_table
    )
    extra_data = Column(MUTABLE_JSON, nullable=True)  # For future data I haven't thought of yet

    def __repr__(self) -> str:
        return (
            f"Video (id={self.id!r}, username={self.username!r}, "
            f"hashtags={self.hashtags!r}, "
            f"crawl_tags={self.crawl_tags!r}, crawls={self.crawls!r}"
        )

    @property
    def hashtag_names(self):
        return {hashtag.name for hashtag in self.hashtags}

    @property
    def crawl_tag_names(self):
        return {crawl_tag.name for crawl_tag in self.crawl_tags}

    @property
    def effect_ids(self):
        return {effect.effect_id for effect in self.effects}


# TODO(macpd): should we track crawl_id <-> User many-to-many relationship?
class UserInfo(Base):
    __tablename__ = "user_info"

    # TODO(macpd): maybe declare relationship to video.username
    username: Mapped[str] = mapped_column(primary_key=True)
    display_name: Mapped[str]
    bio_description: Mapped[str]
    avatar_url: Mapped[str]
    is_verified: Mapped[bool]
    likes_count: Mapped[int]
    video_count: Mapped[int]
    follower_count: Mapped[int]
    following_count: Mapped[int]


# TODO(macpd): should we track crawl_id <-> Comment many-to-many relationship?
class Comment(Base):
    __tablename__ = "comment"

    id: Mapped[int] = mapped_column(
        BigIntegerForPrimaryKeyType, autoincrement=False, primary_key=True
    )
    text: Mapped[str]
    # TODO(macpd): maybe declare relationship to video.id
    video_id: Mapped[int]
    parent_comment_id: Mapped[int]
    like_count: Mapped[int] = mapped_column(nullable=True)
    reply_count: Mapped[int] = mapped_column(nullable=True)
    create_time: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=False))


# TODO(macpd): make generic method for this and use for all many-to-many objects inserted with video
def _get_hashtag_name_to_hashtag_object_map(
    session: Session, video_data: Sequence[Mapping[str, Any]]
) -> Mapping[str, Hashtag]:
    """Gets hashtag name -> Hashtag object map, pulling existing Hashtag objects from database and
    creating new Hashtag objects for new hashtag names.
    """
    # Get all hashtag names references in this list of videos
    hashtag_names_referenced = set(
        itertools.chain.from_iterable([video.get("hashtag_names", []) for video in video_data])
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


def _get_crawl_tag_set(
    session: Session, crawl_tags: Sequence[CrawlTag] | Sequence[str] | None
) -> set[CrawlTag]:
    """Gets crawl_tag name -> CrawlTag object map, pulling existing CrawlTag objects from database
    and creating new CrawlTag objects for new crawl_tag names.
    """
    if not crawl_tags:
        return set()
    crawl_tags = [
        CrawlTag(name=crawl_tag) if isinstance(crawl_tag, str) else crawl_tag
        for crawl_tag in crawl_tags
    ]
    # Get all crawl_tag names references in this list of videos
    crawl_tag_names_referenced = {crawl_tag.name for crawl_tag in crawl_tags}
    # Of all the referenced crawl_tag names get those which exist in the database
    crawl_tag_name_to_crawl_tag = {
        row.name: row
        for row in session.scalars(
            select(CrawlTag).where(CrawlTag.name.in_(crawl_tag_names_referenced))
        )
    }
    # Make new crawl_tag objects for crawl_tag names not yet in the database
    existing_crawl_tag_names = set(crawl_tag_name_to_crawl_tag.keys())
    new_crawl_tag_names = crawl_tag_names_referenced - existing_crawl_tag_names
    for crawl_tag_name in new_crawl_tag_names:
        crawl_tag_name_to_crawl_tag[crawl_tag_name] = CrawlTag(name=crawl_tag_name)
    session.add_all(crawl_tag_name_to_crawl_tag.values())
    return set(crawl_tag_name_to_crawl_tag.values())


def _get_effect_id_to_effect_object_map(
    session: Session, video_data: Sequence[Mapping[str, Any]]
) -> Mapping[str, Effect]:
    """Gets effect id -> Effect object map, pulling existing Effect objects from database and
    creating new Effect objects for new effect ids.
    """
    # Get all effect ids references in this list of videos
    effect_ids_referenced = set(
        itertools.chain.from_iterable([video.get("effect_ids", []) for video in video_data])
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


def upsert_user_info(user_info_sequence: Sequence[Mapping[str, str | int]], engine: Engine):
    with Session(engine) as session:
        for user_info in user_info_sequence:
            new_user = UserInfo(**user_info)
            session.merge(new_user)
        session.commit()


def upsert_comments(comments: Sequence[Mapping[str, str | int]], engine: Engine):
    with Session(engine) as session:
        for comment in comments:
            comment_copy = copy.deepcopy(comment)
            comment_copy["create_time"] = datetime.datetime.fromtimestamp(
                comment_copy["create_time"]
            )
            session.merge(Comment(**comment_copy))
        session.commit()


def upsert_videos(
    video_data: Sequence[dict[str, Any]],
    crawl_id: int,
    engine: Engine,
    crawl_tags: Sequence[CrawlTag] | Sequence[str] | None = None,
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
        hashtag_name_to_hashtag = _get_hashtag_name_to_hashtag_object_map(session, video_data)

        # Get all crawl_tag names references in this list of videos
        crawl_tags_set = _get_crawl_tag_set(session, crawl_tags)

        # Get all effect ids references in this list of videos
        effect_id_to_effect = _get_effect_id_to_effect_object_map(session, video_data)

        video_id_to_video = {}
        crawl = set(session.scalars(select(Crawl).where(Crawl.id == crawl_id)).all())

        for vid in video_data:
            # manually add the source, keeping the original dict intact
            new_vid = copy.deepcopy(vid)
            new_vid["crawls"] = crawl
            new_vid["create_time"] = datetime.datetime.fromtimestamp(vid["create_time"])
            if "effect_ids" in vid:
                new_vid["effects"] = {
                    effect_id_to_effect[effect_id] for effect_id in vid["effect_ids"]
                }
                del new_vid["effect_ids"]
            if "hashtag_names" in vid:
                new_vid["hashtags"] = {
                    hashtag_name_to_hashtag[hashtag_name] for hashtag_name in vid["hashtag_names"]
                }
                del new_vid["hashtag_names"]
            if crawl_tags_set:
                new_vid["crawl_tags"] = crawl_tags_set

            video_id_to_video[vid["id"]] = new_vid

        # Taken from https://stackoverflow.com/questions/25955200/sqlalchemy-performing-a-bulk-upsert-if-exists-update-else-insert-in-postgr

        # Merge all the videos that already exist
        for each in session.scalars(select(Video).where(Video.id.in_(video_id_to_video.keys()))):
            new_vid = Video(**video_id_to_video.pop(each.id))
            new_vid.crawl_tags.update(each.crawl_tags)
            new_vid.crawls.update(each.crawls)
            session.merge(new_vid)

        session.add_all(Video(**vid) for vid in video_id_to_video.values())

        session.commit()


class Crawl(Base):
    __tablename__ = "crawl"

    id: Mapped[int] = mapped_column(
        BigIntegerForPrimaryKeyType, primary_key=True, autoincrement=True
    )

    crawl_started_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    cursor: Mapped[int] = mapped_column(BigInteger)
    has_more: Mapped[bool]
    search_id: Mapped[str]
    query: Mapped[str]

    updated_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), server_default=func.now()
    )

    crawl_tags: Mapped[set[CrawlTag]] = relationship(
        secondary=crawls_to_crawl_tags_association_table
    )
    extra_data = Column(MUTABLE_JSON, nullable=True)  # For future data I haven't thought of yet

    def __repr__(self) -> str:
        return (
            f"Crawl id={self.id}, crawl_tags={self.crawl_tags!r}, "
            f"started_at={self.crawl_started_at!r}, cursor={self.cursor}, "
            f"has_more={self.has_more!r}, search_id={self.search_id!r}\n"
            f"query='{self.query!r}'"
        )

    @classmethod
    def from_request(
        cls, res_data: Mapping, query: VideoQuery | str, crawl_tags: Sequence[str] | None = None
    ) -> "Crawl":
        query_str = None
        if isinstance(query, VideoQuery):
            query_str = json.dumps(query, cls=VideoQueryJSONEncoder)
        else:
            query_str = query
        return cls(
            cursor=res_data["cursor"],
            has_more=res_data["has_more"],
            search_id=res_data["search_id"],
            query=query_str,
            crawl_tags=({CrawlTag(name=name) for name in crawl_tags} if crawl_tags else set()),
        )

    # TODO(macpd): rename this to explain it's intent of being used before fetch starts
    @classmethod
    def from_query(
        cls,
        query: VideoQuery,
        crawl_tags: Sequence[str] | None = None,
        has_more: bool = True,
        search_id: [int | None] = None,
    ) -> "Crawl":
        query_str = None
        if isinstance(query, VideoQuery):
            query_str = json.dumps(query, cls=VideoQueryJSONEncoder)
        else:
            query_str = query
        return cls(
            has_more=has_more,
            query=query_str,
            search_id=search_id,
            crawl_tags=({CrawlTag(name=name) for name in crawl_tags} if crawl_tags else set()),
        )

    def upload_self_to_db(self, engine: Engine) -> None:
        """Uploads current instance to DB"""
        with Session(engine, expire_on_commit=False) as session:
            # Pull CrawlTag with existing names into current session, and then add all to DB
            crawl_tag_name_to_crawl_tag = {
                crawl_tag.name: crawl_tag for crawl_tag in self.crawl_tags
            }
            for each in session.scalars(
                select(CrawlTag).where(CrawlTag.name.in_(crawl_tag_name_to_crawl_tag.keys()))
            ):
                self.crawl_tags.remove(crawl_tag_name_to_crawl_tag.pop(each.name))
                session.merge(each)
                self.crawl_tags.add(each)
            session.add_all(self.crawl_tags)

            if self.id:
                session.merge(self)
            else:
                session.add(self)
            session.commit()


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
