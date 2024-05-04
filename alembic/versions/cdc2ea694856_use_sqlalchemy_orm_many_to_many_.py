"""use SqlAlchemy ORM many-to-many relationships of videos to hashtags, effect IDs, and crawl tags.  add many-to-one for video -> crawl_id. Migraate data from existing columns (which used MyJsonList type) into new schema.

Revision ID: cdc2ea694856
Revises: 
Create Date: 2024-04-23 18:41:36.210381

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "cdc2ea694856"
down_revision: Union[str, None] = "992d4d3bf349"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    dialect_name = conn.dialect.name
    if not dialect_name.startswith('postgresql') and not dialect_name.startswith('sqlite'):
        raise NotImplementedError(f"{dialect_name} not supported!")

    # Make timesamp columns into ones with timezone
    # Batch syntax is used to avoid issues with SQLite
    # See https://alembic.sqlalchemy.org/en/latest/batch.html
    with op.batch_alter_table("crawl", schema=None) as batch_op:
        batch_op.alter_column(
            column_name="crawl_started_at",
            existing_type=postgresql.TIMESTAMP(),
            type_=sa.DateTime(timezone=True),
            existing_nullable=False,
            existing_server_default=sa.text("CURRENT_TIMESTAMP"),
        )

    with op.batch_alter_table("crawl", schema=None) as batch_op:
        batch_op.alter_column(
            column_name="updated_at",
            existing_type=postgresql.TIMESTAMP(),
            type_=sa.DateTime(timezone=True),
            existing_nullable=True,
            existing_server_default=sa.text("CURRENT_TIMESTAMP"),
        )

    with op.batch_alter_table("video", schema=None) as batch_op:
        batch_op.alter_column(
            column_name="crawled_at",
            existing_type=postgresql.TIMESTAMP(),
            type_=sa.DateTime(timezone=True),
            existing_nullable=False,
            existing_server_default=sa.text("CURRENT_TIMESTAMP"),
        )

    with op.batch_alter_table("video", schema=None) as batch_op:
        batch_op.alter_column(
            column_name="crawled_updated_at",
            existing_type=postgresql.TIMESTAMP(),
            type_=sa.DateTime(timezone=True),
            existing_nullable=True,
        )

    # create crawl_tag, crawls_to_crawl_tags, videos_to_crawl_tags
    op.create_table(
        "crawl_tag",
        sa.Column(
            "id", sa.Integer, primary_key=True, autoincrement=True
        ),
        sa.Column(
            "name", sa.String, nullable=False
        ),
    )
    with op.batch_alter_table("crawl_tag", schema=None) as batch_op:
        batch_op.create_unique_constraint(None, ["name"])

    op.create_table(
        "crawls_to_crawl_tags",
        sa.Column(
            "crawl_id", sa.BigInteger, sa.ForeignKey("crawl.id"), primary_key=True
        ),
        sa.Column(
            "crawl_tag_id", sa.Integer, sa.ForeignKey("crawl_tag.id"), primary_key=True
        ),
    )
    op.create_table(
        "videos_to_crawl_tags",
        sa.Column(
            "video_id", sa.BigInteger, sa.ForeignKey("video.id"), primary_key=True
        ),
        sa.Column(
            "crawl_tag_id", sa.Integer, sa.ForeignKey("crawl_tag.id"), primary_key=True
        ),
    )

    op.create_table(
        "effect",
        sa.Column(
            "id", sa.Integer, primary_key=True, autoincrement=True
        ),
        sa.Column(
            "effect_id", sa.String, nullable=False
        ),
    )
    with op.batch_alter_table("effect", schema=None) as batch_op:
        batch_op.create_unique_constraint(None, ["effect_id"])
    op.create_table(
        "videos_to_effect_ids",
        sa.Column(
            "video_id", sa.BigInteger, sa.ForeignKey("video.id"), primary_key=True
        ),
        sa.Column(
            "effect_id", sa.Integer, sa.ForeignKey("effect.id"), primary_key=True
        ),
    )

    op.create_table(
        "hashtag",
        sa.Column(
            "id", sa.Integer, primary_key=True, autoincrement=True
        ),
        sa.Column(
            "name", sa.String, nullable=False
        ),
    )

    op.create_table(
        "videos_to_hashtags",
        sa.Column(
            "video_id", sa.BigInteger, sa.ForeignKey("video.id"), primary_key=True
        ),
        sa.Column(
            "hashtag_id", sa.BigInteger, sa.ForeignKey("hashtag.id"), primary_key=True
        ),
    )


    with op.batch_alter_table("hashtag", schema=None) as batch_op:
        # Use new naming convention for constraints
        #  batch_op.drop_constraint("hashtag_name_key", type_="unique")
        batch_op.create_unique_constraint(None, ["name"])

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
    conn = op.get_bind()
    dialect_name = conn.dialect.name
    if dialect_name.startswith('postgresql'):
        source_values_json_unnest_statment = (
                f"SELECT DISTINCT json_array_elements_text({source_table_value_column}->'list') "
                f"FROM {source_table_name}")
        association_table_source_id_column_and_source_values_json_unnest_statment = (
                f"SELECT {source_table_name}.{source_table_id_column} AS {association_table_source_id_column}, "
                f"json_array_elements_text({source_table_value_column}->'list') AS value "
                f"FROM {source_table_name}"
                )
    elif dialect_name.startswith('sqlite'):
        source_values_json_unnest_statment = (
                f"SELECT DISTINCT j.value FROM {source_table_name}, "
                f"json_each({source_table_name}.{source_table_value_column}->'list') AS j "
                f"WHERE j.value IS NOT NULL AND j.value != ''")
        association_table_source_id_column_and_source_values_json_unnest_statment = (
                f"SELECT {source_table_name}.{source_table_id_column} AS {association_table_source_id_column}, "
                f"j.value FROM {source_table_name}, "
                f"json_each({source_table_name}.{source_table_value_column}->'list') AS j "
                )


    # Make sure new table has all existing values
    op.execute(
        f"INSERT INTO {new_value_table_name} ({new_value_table_value_column}) "
        f"{source_values_json_unnest_statment} "
        f"ON CONFLICT ({new_value_table_value_column}) DO NOTHING;"
    )
    # Get list of association_table_source_id_column with values (extracted from
    # source_table_value_column->'list'), join it with new_value_table_name on
    # new_value_table_value_column, and insert (association_table_source_id_column,
    # association_table_value_id_column) into association_table_name
    op.execute(
        f"INSERT INTO {association_table_name} ({association_table_source_id_column}, {association_table_value_id_column}) "
        f"SELECT source_id_to_value.{association_table_source_id_column}, {new_value_table_name}.{new_value_table_id_column} FROM "
        f"({association_table_source_id_column_and_source_values_json_unnest_statment}) AS source_id_to_value "
        f"JOIN {new_value_table_name} ON (source_id_to_value.value = {new_value_table_name}.{new_value_table_value_column}) "
        f"ON CONFLICT ({association_table_source_id_column}, {association_table_value_id_column}) DO NOTHING;"
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
    conn = op.get_bind()
    dialect_name = conn.dialect.name
    if dialect_name.startswith('postgresql'):
        json_list_aggregate_statement = f"json_build_object('list', json_agg(value_to_aggregate))"
    elif dialect_name.startswith('sqlite'):
        json_list_aggregate_statement = f"json_object('list', json_group_array(a.value_to_aggregate))"
    op.execute(
        f"WITH source_id_to_json_list AS ( "
        f"  SELECT a.{association_table_source_id_column}, {json_list_aggregate_statement} AS json_list FROM "
        f"  ( "
        f"      SELECT {association_table_name}.{association_table_source_id_column}, {new_value_table_name}.{new_value_table_value_column} AS value_to_aggregate "
        f"      FROM {association_table_name} JOIN {new_value_table_name} ON({association_table_name}.{association_table_value_id_column} = {new_value_table_name}.{new_value_table_id_column}) "
        f"  ) as a GROUP BY a.{association_table_source_id_column} "
        f") "
        f"UPDATE {source_table_name} SET {source_table_value_column} = source_id_to_json_list.json_list FROM source_id_to_json_list WHERE {source_table_name}.{source_table_id_column} = source_id_to_json_list.{association_table_source_id_column}"
    )


def revert_crawl_source_column_data_from_crawls_to_crawl_tags():
    revert_association_table_data_to_json_list_type(
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


def revert_video_source_column_data_from_videos_to_crawl_tags():
    revert_association_table_data_to_json_list_type(
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
    with op.batch_alter_table("video", schema=None) as batch_op:
        batch_op.alter_column(
            "crawled_updated_at",
            existing_type=sa.DateTime(timezone=True),
            type_=postgresql.TIMESTAMP(),
            existing_nullable=True,
        )
    with op.batch_alter_table("video", schema=None) as batch_op:
        batch_op.alter_column(
            "crawled_at",
            existing_type=sa.DateTime(timezone=True),
            type_=postgresql.TIMESTAMP(),
            existing_nullable=False,
            existing_server_default=sa.text("CURRENT_TIMESTAMP"),
        )

    op.add_column(
        "crawl",
        sa.Column(
            "source",
            postgresql.JSON(astext_type=sa.Text()),
            autoincrement=False,
            nullable=True,
        ),
    )
    with op.batch_alter_table("crawl", schema=None) as batch_op:
        batch_op.alter_column(
            "updated_at",
            existing_type=sa.DateTime(timezone=True),
            type_=postgresql.TIMESTAMP(),
            existing_nullable=True,
            existing_server_default=sa.text("CURRENT_TIMESTAMP"),
        )
        batch_op.alter_column(
            "crawl_started_at",
            existing_type=sa.DateTime(timezone=True),
            type_=postgresql.TIMESTAMP(),
            existing_nullable=False,
            existing_server_default=sa.text("CURRENT_TIMESTAMP"),
        )
        batch_op.alter_column(
            "id",
            existing_type=sa.Integer(),
            type_=sa.BIGINT(),
            existing_nullable=False,
            autoincrement=True,
        )

    revert_crawl_source_column_data_from_crawls_to_crawl_tags()
    revert_video_source_column_data_from_videos_to_crawl_tags()
    revert_video_hashtag_names_column_data_from_videos_to_hashtags()
    revert_video_effect_ids_column_data_from_videos_to_effect_ids()

    op.drop_table("videos_to_hashtags")
    op.drop_table("videos_to_crawls")
    op.drop_table("crawls_to_crawl_tags")
    op.drop_table("videos_to_crawl_tags")
    op.drop_table("videos_to_effect_ids")
    op.drop_table("hashtag")
    op.drop_table("effect")
    op.drop_table("crawl_tag")
