"""Remove query_tags, crawls_to_query_tags, videos_to_query_tags, effect, and videos_to_effect_ids tables if they exist.
THIS IS IRREVERISBLE AND DOES NOT RECREATE TABLES ON DOWNGRADE

Revision ID: 992d4d3bf349
Revises: cdc2ea694856
Create Date: 2024-05-02 21:06:21.695412

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector


# revision identifiers, used by Alembic.
revision: str = "992d4d3bf349"
down_revision: Union[str, None] = "ee22882184dd"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    tables = inspector.get_table_names()
    if "query_tag" in tables:
        op.drop_table("videos_to_query_tags")
        op.drop_table("crawls_to_query_tags")
        op.drop_table("query_tag")

    if "effect" in tables:
        op.drop_table("videos_to_effect_ids")
        op.drop_table("effect")

    if "hashtag" in tables:
        op.drop_table("videos_to_hashtags")
        op.drop_table("hashtag")

def downgrade() -> None:
    pass
