"""rename Crawl -> crawl. DOES NOT RENAME ON DOWNGRADE

Revision ID: ee22882184dd
Revises: cdc2ea694856
Create Date: 2024-05-03 15:12:53.777865

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector


# revision identifiers, used by Alembic.
revision: str = "ee22882184dd"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    tables = inspector.get_table_names()
    if "Crawl" in tables:
        # Rename to a temp location b/c sqlite table names are effectively case insensitive.
        op.rename_table("Crawl", "_tmp_crawl")
        op.rename_table("_tmp_crawl", "crawl")



def downgrade() -> None:
    pass
