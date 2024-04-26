"""use SqlAlchemy ORM many-to-many relationships of videos to hashtags, effect IDs, and crawl tags.  add many-to-one for video -> crawl_id. rename query_tags -> crawl_tags. Migraate data from existing columns (which used MyJsonList type) into new schema.

Revision ID: cdc2ea694856
Revises: 
Create Date: 2024-04-23 18:41:36.210381

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "cdc2ea694856"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Make timesamp columns into ones with timezone
    op.alter_column(
        "crawl",
        "crawl_started_at",
        existing_type=postgresql.TIMESTAMP(),
        type_=sa.DateTime(timezone=True),
        existing_nullable=False,
        existing_server_default=sa.text("CURRENT_TIMESTAMP"),
    )
    op.alter_column(
        "crawl",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        type_=sa.DateTime(timezone=True),
        existing_nullable=True,
        existing_server_default=sa.text("CURRENT_TIMESTAMP"),
    )
    op.alter_column(
        "video",
        "crawled_at",
        existing_type=postgresql.TIMESTAMP(),
        type_=sa.DateTime(timezone=True),
        existing_nullable=False,
        existing_server_default=sa.text("CURRENT_TIMESTAMP"),
    )
    op.alter_column(
        "video",
        "crawled_updated_at",
        existing_type=postgresql.TIMESTAMP(),
        type_=sa.DateTime(timezone=True),
        existing_nullable=True,
    )

    # Rename query_tag -> crawl_tag table and all references to query_tag in columns, primary key, and unique
    # constraint
    op.rename_table("query_tag", "crawl_tag")

    # Drop foreign key refs that rely on primary key we want to rename
    op.drop_constraint(
        constraint_name="crawls_to_query_tags_crawl_id_fkey",
        table_name="crawls_to_query_tags",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="crawls_to_query_tags_query_tag_id_fkey",
        table_name="crawls_to_query_tags",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="videos_to_query_tags_query_tag_id_fkey",
        table_name="videos_to_query_tags",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="videos_to_query_tags_video_id_fkey",
        table_name="videos_to_query_tags",
        type_="foreignkey",
    )

    op.drop_constraint("query_tag_pkey", "crawl_tag", type_="primary")
    op.drop_constraint("query_tag_name_key", "crawl_tag", type_="unique")
    op.create_primary_key(constraint_name=None, table_name="crawl_tag", columns=["id"])
    op.create_unique_constraint(
        constraint_name=None, table_name="crawl_tag", columns=["name"]
    )

    # Rename crawls_to_query_tags -> crawls_to_crawl_tags in table and all references to query_tag in columns, primary key, and unique
    # constraint
    op.rename_table("crawls_to_query_tags", "crawls_to_crawl_tags")
    op.alter_column(
        table_name="crawls_to_crawl_tags",
        column_name="query_tag_id",
        new_column_name="crawl_tag_id",
    )
    op.drop_constraint(
        constraint_name="crawls_to_query_tags_pkey",
        table_name="crawls_to_crawl_tags",
        type_="primary",
    )
    op.create_primary_key(
        constraint_name=None,
        table_name="crawls_to_crawl_tags",
        columns=["crawl_id", "crawl_tag_id"],
    )
    op.create_foreign_key(
        constraint_name=None,
        source_table="crawls_to_crawl_tags",
        referent_table="crawl",
        local_cols=["crawl_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name=None,
        source_table="crawls_to_crawl_tags",
        referent_table="crawl_tag",
        local_cols=["crawl_tag_id"],
        remote_cols=["id"],
    )

    # Rename videod_to_query_tags -> videod_to_crawl_tags in table and all references to query_tag in columns, primary key, and unique
    # constraint
    op.rename_table("videos_to_query_tags", "videos_to_crawl_tags")
    op.alter_column(
        table_name="videos_to_crawl_tags",
        column_name="query_tag_id",
        new_column_name="crawl_tag_id",
    )
    op.drop_constraint(
        constraint_name="videos_to_query_tags_pkey",
        table_name="videos_to_crawl_tags",
        type_="primary",
    )
    op.create_primary_key(
        constraint_name=None,
        table_name="videos_to_crawl_tags",
        columns=["video_id", "crawl_tag_id"],
    )
    op.create_foreign_key(
        constraint_name=None,
        source_table="videos_to_crawl_tags",
        referent_table="video",
        local_cols=["video_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name=None,
        source_table="videos_to_crawl_tags",
        referent_table="crawl_tag",
        local_cols=["crawl_tag_id"],
        remote_cols=["id"],
    )

    # Use new naming convention for constraints
    op.drop_constraint("effect_effect_id_key", "effect", type_="unique")
    op.create_unique_constraint(op.f("effect_effect_id_uniq"), "effect", ["effect_id"])

    op.drop_constraint("hashtag_name_key", "hashtag", type_="unique")
    op.create_unique_constraint(op.f("hashtag_name_uniq"), "hashtag", ["name"])

    op.create_table(
        "videos_to_crawls",
        sa.Column(
            "video_id", sa.BigInteger, sa.ForeignKey("video.id"), primary_key=True
        ),
        sa.Column(
            "crawl_id", sa.BigInteger, sa.ForeignKey("crawl.id"), primary_key=True
        ),
    )

    # Migrate crawl.source data to crawls_to_crawl_tags association table
    migrate_crawl_source_column_data_to_crawls_to_crawl_tags()

    # Migrate data from fields that used MyJsonList type to respective association tables, and then
    # drop the columns
    migrate_video_source_column_data_to_videos_to_crawl_tags()
    migrate_video_hashtag_names_column_data_to_videos_to_hashtags()
    migrate_video_effect_ids_column_data_to_videos_to_effect_ids()

    op.drop_column("crawl", "source")
    op.drop_column("video", "source")
    op.drop_column("video", "hashtag_names")
    op.drop_column("video", "effect_ids")


def migrate_source_column_data_to_association_table(
    association_table_name,
    association_table_source_id_column,
    association_table_value_id_column,
    new_value_table_name,
    new_value_table_id_column,
    new_value_table_value_column,
    source_table_name,
    source_table_id_column,
    source_table_value_column,
):
    # Make sure new table has all existing values
    op.execute(
        f"INSERT INTO {new_value_table_name} ({new_value_table_value_column}) "
        f"SELECT DISTINCT json_array_elements_text({source_table_value_column}->'list') FROM {source_table_name} "
        f"ON CONFLICT ({new_value_table_value_column}) DO NOTHING;"
    )
    # Get list of association_table_source_id_column with values (extracted from
    # source_table_value_column->'list'), join it with new_value_table_name on
    # new_value_table_value_column, and insert (association_table_source_id_column,
    # association_table_value_id_column) into association_table_name
    op.execute(
        f"INSERT INTO {association_table_name} ({association_table_source_id_column}, {association_table_value_id_column}) "
        f"SELECT source_id_to_value.{association_table_source_id_column}, {new_value_table_name}.{new_value_table_id_column} FROM "
        f"    (SELECT {source_table_id_column} AS {association_table_source_id_column}, json_array_elements_text({source_table_value_column}->'list') as value"
        f"     FROM {source_table_name}) AS source_id_to_value "
        f"JOIN {new_value_table_name} ON (source_id_to_value.value = {new_value_table_name}.{new_value_table_value_column}) "
        f"ON CONFLICT ({association_table_source_id_column}, {association_table_value_id_column}) DO NOTHING"
    )


def migrate_crawl_source_column_data_to_crawls_to_crawl_tags():
    migrate_source_column_data_to_association_table(
        association_table_name="crawls_to_crawl_tags",
        association_table_source_id_column="crawl_id",
        association_table_value_id_column="crawl_tag_id",
        new_value_table_name="crawl_tag",
        new_value_table_id_column="id",
        new_value_table_value_column="name",
        source_table_name="crawl",
        source_table_id_column="id",
        source_table_value_column="source",
    )


def migrate_video_source_column_data_to_videos_to_crawl_tags():
    migrate_source_column_data_to_association_table(
        association_table_name="videos_to_crawl_tags",
        association_table_source_id_column="video_id",
        association_table_value_id_column="crawl_tag_id",
        new_value_table_name="crawl_tag",
        new_value_table_id_column="id",
        new_value_table_value_column="name",
        source_table_name="video",
        source_table_id_column="id",
        source_table_value_column="source",
    )


def migrate_video_hashtag_names_column_data_to_videos_to_hashtags():
    migrate_source_column_data_to_association_table(
        association_table_name="videos_to_hashtags",
        association_table_source_id_column="video_id",
        association_table_value_id_column="hashtag_id",
        new_value_table_name="hashtag",
        new_value_table_id_column="id",
        new_value_table_value_column="name",
        source_table_name="video",
        source_table_id_column="id",
        source_table_value_column="hashtag_names",
    )


def migrate_video_effect_ids_column_data_to_videos_to_effect_ids():
    migrate_source_column_data_to_association_table(
        association_table_name="videos_to_effect_ids",
        association_table_source_id_column="video_id",
        association_table_value_id_column="effect_id",
        new_value_table_name="effect",
        new_value_table_id_column="id",
        new_value_table_value_column="effect_id",
        source_table_name="video",
        source_table_id_column="id",
        source_table_value_column="effect_ids",
    )


def revert_association_table_data_to_json_list_type(
    association_table_name,
    association_table_source_id_column,
    association_table_value_id_column,
    new_value_table_name,
    new_value_table_id_column,
    new_value_table_value_column,
    source_table_name,
    source_table_id_column,
    source_table_value_column,
):
    op.execute(
        f"WITH source_id_to_json_list AS ( "
        f"SELECT {association_table_source_id_column}, json_build_object('list', json_agg(value_to_aggregate)) AS json_list FROM "
        f"( "
        f"    SELECT {association_table_source_id_column}, {new_value_table_name}.{new_value_table_value_column} AS value_to_aggregate FROM {association_table_name} JOIN {new_value_table_name} ON({association_table_name}.{association_table_value_id_column} = {new_value_table_name}.{new_value_table_id_column}) "
        f") AS a GROUP BY {association_table_source_id_column} "
        f") "
        f"UPDATE {source_table_name} SET {source_table_value_column} = source_id_to_json_list.json_list FROM source_id_to_json_list WHERE {source_table_name}.{source_table_id_column} = source_id_to_json_list.{association_table_source_id_column}"
    )


def revert_crawl_source_column_data_from_crawls_to_query_tags():
    revert_association_table_data_to_json_list_type(
        association_table_name="crawls_to_query_tags",
        association_table_source_id_column="crawl_id",
        association_table_value_id_column="query_tag_id",
        new_value_table_name="query_tag",
        new_value_table_id_column="id",
        new_value_table_value_column="name",
        source_table_name="crawl",
        source_table_id_column="id",
        source_table_value_column="source",
    )


def revert_video_source_column_data_from_videos_to_query_tags():
    revert_association_table_data_to_json_list_type(
        association_table_name="videos_to_query_tags",
        association_table_source_id_column="video_id",
        association_table_value_id_column="query_tag_id",
        new_value_table_name="query_tag",
        new_value_table_id_column="id",
        new_value_table_value_column="name",
        source_table_name="video",
        source_table_id_column="id",
        source_table_value_column="source",
    )


def revert_video_hashtag_names_column_data_from_videos_to_hashtags():
    revert_association_table_data_to_json_list_type(
        association_table_name="videos_to_hashtags",
        association_table_source_id_column="video_id",
        association_table_value_id_column="hashtag_id",
        new_value_table_name="hashtag",
        new_value_table_id_column="id",
        new_value_table_value_column="name",
        source_table_name="video",
        source_table_id_column="id",
        source_table_value_column="hashtag_names",
    )


def revert_video_effect_ids_column_data_from_videos_to_effect_ids():
    revert_association_table_data_to_json_list_type(
        association_table_name="videos_to_effect_ids",
        association_table_source_id_column="video_id",
        association_table_value_id_column="effect_id",
        new_value_table_name="effect",
        new_value_table_id_column="id",
        new_value_table_value_column="effect_id",
        source_table_name="video",
        source_table_id_column="id",
        source_table_value_column="effect_ids",
    )


def downgrade() -> None:
    op.add_column(
        "video",
        sa.Column(
            "effect_ids",
            postgresql.JSON(astext_type=sa.Text()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "video",
        sa.Column(
            "source",
            postgresql.JSON(astext_type=sa.Text()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "video",
        sa.Column(
            "hashtag_names",
            postgresql.JSON(astext_type=sa.Text()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.alter_column(
        "video",
        "crawled_updated_at",
        existing_type=sa.DateTime(timezone=True),
        type_=postgresql.TIMESTAMP(),
        existing_nullable=True,
    )
    op.alter_column(
        "video",
        "crawled_at",
        existing_type=sa.DateTime(timezone=True),
        type_=postgresql.TIMESTAMP(),
        existing_nullable=False,
        existing_server_default=sa.text("CURRENT_TIMESTAMP"),
    )
    op.drop_constraint(op.f("hashtag_name_uniq"), "hashtag", type_="unique")
    op.create_unique_constraint("hashtag_name_key", "hashtag", ["name"])
    op.drop_constraint(op.f("effect_effect_id_uniq"), "effect", type_="unique")
    op.create_unique_constraint("effect_effect_id_key", "effect", ["effect_id"])
    op.add_column(
        "crawl",
        sa.Column(
            "source",
            postgresql.JSON(astext_type=sa.Text()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.alter_column(
        "crawl",
        "updated_at",
        existing_type=sa.DateTime(timezone=True),
        type_=postgresql.TIMESTAMP(),
        existing_nullable=True,
        existing_server_default=sa.text("CURRENT_TIMESTAMP"),
    )
    op.alter_column(
        "crawl",
        "crawl_started_at",
        existing_type=sa.DateTime(timezone=True),
        type_=postgresql.TIMESTAMP(),
        existing_nullable=False,
        existing_server_default=sa.text("CURRENT_TIMESTAMP"),
    )
    op.alter_column(
        "crawl",
        "id",
        existing_type=sa.Integer(),
        type_=sa.BIGINT(),
        existing_nullable=False,
        autoincrement=True,
    )
    # Drop FK references so the primary keys they rely on can be dropped/added back.
    op.drop_constraint(
        constraint_name="crawls_to_crawl_tags_crawl_id_crawl_fkey",
        table_name="crawls_to_crawl_tags",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="crawls_to_crawl_tags_crawl_tag_id_crawl_tag_fkey",
        table_name="crawls_to_crawl_tags",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="videos_to_crawl_tags_video_id_video_fkey",
        table_name="videos_to_crawl_tags",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="videos_to_crawl_tags_crawl_tag_id_crawl_tag_fkey",
        table_name="videos_to_crawl_tags",
        type_="foreignkey",
    )

    # Rename crawl_tag -> query_tag table and all references to crawl_tag in columns, primary key, and unique
    # constraint
    op.rename_table("crawl_tag", "query_tag")
    op.drop_constraint("crawl_tag_pkey", "query_tag", type_="primary")
    op.drop_constraint("crawl_tag_name_uniq", "query_tag", type_="unique")
    op.create_primary_key(constraint_name=None, table_name="query_tag", columns=["id"])
    op.create_unique_constraint(
        constraint_name="query_tag_name_key", table_name="query_tag", columns=["name"]
    )

    # Rename crawls_to_crawl_tags -> crawls_to_query_tags in table and all references to crawl_tag in columns, primary key, and unique
    # constraint
    op.rename_table("crawls_to_crawl_tags", "crawls_to_query_tags")
    op.alter_column(
        table_name="crawls_to_query_tags",
        column_name="crawl_tag_id",
        new_column_name="query_tag_id",
    )
    op.drop_constraint(
        constraint_name="crawls_to_crawl_tags_pkey",
        table_name="crawls_to_query_tags",
        type_="primary",
    )
    op.create_primary_key(
        constraint_name=None,
        table_name="crawls_to_query_tags",
        columns=["crawl_id", "query_tag_id"],
    )
    op.create_foreign_key(
        constraint_name="crawls_to_query_tags_crawl_id_fkey",
        source_table="crawls_to_query_tags",
        referent_table="crawl",
        local_cols=["crawl_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name="crawls_to_query_tags_query_tag_id_fkey",
        source_table="crawls_to_query_tags",
        referent_table="query_tag",
        local_cols=["query_tag_id"],
        remote_cols=["id"],
    )

    # Rename videod_to_crawl_tags -> videod_to_query_tags in table and all references to crawl_tag in columns, primary key, and unique
    # constraint
    op.rename_table("videos_to_crawl_tags", "videos_to_query_tags")
    op.alter_column(
        table_name="videos_to_query_tags",
        column_name="crawl_tag_id",
        new_column_name="query_tag_id",
    )
    op.drop_constraint(
        constraint_name="videos_to_crawl_tags_pkey",
        table_name="videos_to_query_tags",
        type_="primary",
    )
    op.create_primary_key(
        constraint_name=None,
        table_name="videos_to_query_tags",
        columns=["video_id", "query_tag_id"],
    )
    op.create_foreign_key(
        constraint_name="videos_to_query_tags_video_id_fkey",
        source_table="videos_to_query_tags",
        referent_table="video",
        local_cols=["video_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name="videos_to_query_tags_query_tag_id_fkey",
        source_table="videos_to_query_tags",
        referent_table="query_tag",
        local_cols=["query_tag_id"],
        remote_cols=["id"],
    )

    revert_crawl_source_column_data_from_crawls_to_query_tags()
    revert_video_source_column_data_from_videos_to_query_tags()
    revert_video_hashtag_names_column_data_from_videos_to_hashtags()
    revert_video_effect_ids_column_data_from_videos_to_effect_ids()

    op.drop_table("videos_to_crawls")
