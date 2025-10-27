"""add_key_prefix_to_api_keys

Revision ID: c4c9cb91a45b
Revises: a12ae57a3abf
Create Date: 2025-10-26 21:20:07.272655

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c4c9cb91a45b'
down_revision: Union[str, Sequence[str], None] = 'a12ae57a3abf'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add key_prefix column to api_keys table
    op.add_column('api_keys', sa.Column('key_prefix', sa.String(length=16), nullable=False, comment='First 8 characters of API key for quick lookup'))
    op.create_index('idx_api_keys_prefix', 'api_keys', ['key_prefix'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    # Remove key_prefix column from api_keys table
    op.drop_index('idx_api_keys_prefix', table_name='api_keys')
    op.drop_column('api_keys', 'key_prefix')
