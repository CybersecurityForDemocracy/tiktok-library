"""use SqlAlchemy ORM many-to-many relationships of hashtags, effect IDs, query tags, and many-to-one for video -> crawl_id.

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
    # Make crawl.id a BigInteger
    op.alter_column(
        "crawl",
        "id",
        existing_type=sa.BIGINT(),
        type_=sa.Integer(),
        existing_nullable=False,
        autoincrement=True,
    )
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

    # Migrate crawl.source data to crawls_to_crawl_tags association table
    migrate_crawl_source_column_data_to_crawls_to_crawl_tags()
    op.drop_column("crawl", "source")

    # Rename query_tag -> crawl_tag table and all references to query_tag in columns, primary key, and unique
    # constraint
    op.rename_table("query_tag", "crawl_tag")
    op.drop_constraint("query_tag_pkey", "crawl_tag", type_="priamry")
    op.drop_constraint("query_tag_name_key", "crawl_tag", type_="unique")
    op.create_primary_key(name=None, table_name="crawl_tag", columns=["id"])
    op.create_unique_constraint(name=None, table_name="crawl_tag", ["name"])

    # Rename crawls_to_query_tags -> crawls_to_crawl_tags in table and all references to query_tag in columns, primary key, and unique
    # constraint
    op.rename_table("crawls_to_query_tags", "crawls_to_crawl_tags")
    op.alter_column(table_name="crawls_to_crawl_tags", column_name="query_tag_id",
                    new_column_name="crawl_tag_id")
    op.drop_constraint(constraint_name="crawls_to_query_tags_pkey", table_name="crawls_to_crawl_tags", type_="priamry")
    op.create_primary_key(name=None, table_name="crawls_to_crawl_tags", columns=["crawl_id",
                                                                                 "crawl_tag_id"])
    op.drop_constraint(constraint_name="crawls_to_query_tags_crawl_id_fkey",
                       table_name="crawls_to_crawl_tags", type_="foreignkey")
    op.create_foreign_key(constraint_name=None,
                          source_table="crawls_to_crawl_tags",
                          referent_table="crawl",
                          local_cols=["crawl_id"],
                          remove_cols=["id"])
    op.drop_constraint(constraint_name="crawls_to_query_tags_query_tag_id_fkey",
                       table_name="crawls_to_crawl_tags", type_="foreignkey")
    op.create_foreign_key(constraint_name=None,
                          source_table="crawl_tag",
                          referent_table="crawl",
                          local_cols=["crawl_tag_id"],
                          remove_cols=["id"])

    # Rename crawls_to_query_tags -> crawls_to_crawl_tags in table and all references to query_tag in columns, primary key, and unique
    # constraint
    op.rename_table("videos_to_query_tags", "videos_to_crawl_tags")
    op.alter_column(table_name="videos_to_crawl_tags", column_name="query_tag_id",
                    new_column_name="crawl_tag_id")
    op.drop_constraint(constraint_name="videos_to_query_tags_pkey", table_name="videos_to_crawl_tags", type_="priamry")
    op.create_primary_key(name=None, table_name="videos_to_crawl_tags", columns=["video_id",
                                                                                 "crawl_tag_id"])
    op.drop_constraint(constraint_name="videos_to_query_tags_video_id_fkey",
                       table_name="videos_to_crawl_tags", type_="foreignkey")
    op.create_foreign_key(constraint_name=None,
                          source_table="videos_to_crawl_tags",
                          referent_table="video",
                          local_cols=["video_id"],
                          remove_cols=["id"])
    op.drop_constraint(constraint_name="videos_to_query_tags_query_tag_id_fkey",
                       table_name="videos_to_crawl_tags", type_="foreignkey")
    op.create_foreign_key(constraint_name=None,
                          source_table="crawl_tag",
                          referent_table="crawl",
                          local_cols=["crawl_tag_id"],
                          remove_cols=["id"])



    # Use new naming convention for constraints
    op.drop_constraint("effect_effect_id_key", "effect", type_="unique")
    op.create_unique_constraint(
        op.f("effect_effect_id_uniq"), "effect", ["effect_id"]
    )

    op.drop_constraint("hashtag_name_key", "hashtag", type_="unique")
    op.create_unique_constraint(op.f("hashtag_name_uniq"), "hashtag", ["name"])


    # Add FK reference to crawl_id in video table
    op.add_column("video", sa.Column("crawl_id", sa.Integer(), nullable=False))
    op.create_foreign_key(
        op.f("video_crawl_id_crawl_fkey"),
        "video",
        "crawl",
        ["crawl_id"],
        ["id"],
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

    # Migrate data from fields that used MyJsonList type to respective association tables, and then
    # drop the columns
    migrate_video_hashtag_names_column_data_to_videos_to_hashtags()
    migrate_video_source_column_data_to_videos_to_crawl_tags()
    migrate_video_effect_ids_column_data_to_videos_to_effect_ids()

    op.drop_column("video", "hashtag_names")
    op.drop_column("video", "source")
    op.drop_column("video", "effect_ids")

def migrate_crawl_source_column_data_to_crawls_to_crawl_tags():
    # Make sure crawl_tag has all existing source names
    op.execute("INSERT INTO crawl_tag (name) "
               "SELECT DISTINCT json_array_elements_text(source->'list') FROM crawl "
               "ON CONFLICT (name) DO NOTHING;")
    # Get list of crawl_id with crawl_tag (extracted from source->'list'), join it with crawl_tag on
    # name, and insert (crawl_id, crawl_tag_id) into crawls_to_crawl_tags
    op.execute("INSERT INTO crawls_to_crawl_tags (crawl_id, crawl_tag_id) "
               "SELECT crawl_id, crawl_tag.id FROM "
               "    (SELECT id AS crawl_id, json_array_elements_text(source->'list') as crawl_tag "
               "     FROM crawl) AS crawl_sources "
               "JOIN crawl_tag ON (crawl_sources.crawl_tag = crawl_tag.name)")

def migrate_video_source_column_data_to_videos_to_crawl_tags():
    # Make sure crawl_tag has all existing source names
    op.execute("INSERT INTO crawl_tag (name) "
               "SELECT DISTINCT json_array_elements_text(source->'list') FROM video "
               "ON CONFLICT (name) DO NOTHING;")
    # Get list of video_id with crawl_tag (extracted from source->'list'), join it with crawl_tag on
    # name, and insert (video_id, crawl_tag_id) into videos_to_crawl_tags
    op.execute("INSERT INTO videos_to_crawl_tags (video_id, crawl_tag_id) "
               "SELECT video_id, crawl_tag.id FROM "
               "    (SELECT id AS video_id, json_array_elements_text(source->'list') as crawl_tag "
               "     FROM video) AS video_sources "
               "JOIN crawl_tag ON (video_sources.crawl_tag = crawl_tag.name)")

def migrate_video_hashtag_names_column_data_to_videos_to_hashtags():
    op.execute("INSERT INTO hashtag (name) SELECT DISTINCT json_array_elements_text(hashtag_names->'list') FROM video ON CONFLICT (name) DO NOTHING;")
    op.execute("INSERT INTO videos_to_hashtags (video_id, hashtag_id) SELECT video_id, hashtag.id (SELECT id AS video_id, json_array_elements_text(hashtag_names->'list') AS hashtag_name FROM video) AS video_hashtags FROM video JOIN hashtag ON (video_hashtags.hashtag_name = hashtag.name);")

def migrate_video_effect_ids_column_data_to_videos_to_effect_ids():
    op.execute("INSERT INTO effect (effect_id) SELECT DISTINCT json_array_elements_text(effect_ids->'list') FROM video WHERE ON CONFLICT (name) DO NOTHING;")
    op.execute("INSERT INTO videos_to_effect_ids (video_id, effect_id) SELECT video_id, effect_id.id (SELECT id AS video_id, json_array_elements_text(effect_ids->'list') AS effect_id FROM video) AS video_effect_ids FROM video JOIN effect ON (video_effect_ids.effect_id = effect.effect_id);")


# TODO(macpd): implement downgrade data migration from association tables to MyJsonList types.
def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
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
    op.drop_constraint(
        op.f("video_crawl_id_crawl_fkey"), "video", type_="foreignkey"
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
    op.drop_column("video", "crawl_id")
    op.drop_constraint(op.f("hashtag_name_uniq"), "hashtag", type_="unique")
    op.create_unique_constraint("hashtag_name_key", "hashtag", ["name"])
    op.drop_constraint(op.f("effect_effect_id_uniq"), "effect", type_="unique")
    op.create_unique_constraint(
        "effect_effect_id_key", "effect", ["effect_id"]
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
    op.create_table(
        "videos_to_query_tags",
        sa.Column(
            "video_id", sa.BIGINT(), autoincrement=False, nullable=False
        ),
        sa.Column(
            "query_tag_id", sa.INTEGER(), autoincrement=False, nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["query_tag_id"],
            ["query_tag.id"],
            name="videos_to_query_tags_query_tag_id_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["video_id"],
            ["video.id"],
            name="videos_to_query_tags_video_id_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "video_id", "query_tag_id", name="videos_to_query_tags_pkey"
        ),
    )
    op.create_table(
        "query_tag",
        sa.Column(
            "id",
            sa.INTEGER(),
            server_default=sa.text("nextval('query_tag_id_seq'::regclass)"),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("name", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.PrimaryKeyConstraint("id", name="query_tag_pkey"),
        sa.UniqueConstraint("name", name="query_tag_name_key"),
        postgresql_ignore_search_path=False,
    )
    op.create_table(
        "crawls_to_query_tags",
        sa.Column(
            "crawl_id", sa.INTEGER(), autoincrement=False, nullable=False
        ),
        sa.Column(
            "query_tag_id", sa.INTEGER(), autoincrement=False, nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["crawl_id"],
            ["crawl.id"],
            name="crawls_to_query_tags_crawl_id_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["query_tag_id"],
            ["query_tag.id"],
            name="crawls_to_query_tags_query_tag_id_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "crawl_id", "query_tag_id", name="crawls_to_query_tags_pkey"
        ),
    )
    op.drop_table("videos_to_crawl_tags")
    op.drop_table("crawls_to_crawl_tags")
    op.drop_table("crawl_tag")
    # ### end Alembic commands ###
