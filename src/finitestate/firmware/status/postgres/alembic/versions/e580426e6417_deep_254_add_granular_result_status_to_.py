"""DEEP-254 Add granular result status to database

Revision ID: e580426e6417
Revises: 35a87c4a6be4
Create Date: 2020-07-24 12:29:35.721218

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

# revision identifiers, used by Alembic.
revision = 'e580426e6417'
down_revision = '35a87c4a6be4'
branch_labels = None
depends_on = None


def upgrade():
    # yapf: disable
    op.create_table('result_status',
                    sa.Column('status_id', UUID(), nullable=False),
                    sa.Column('result_code', sa.Text(), nullable=False),
                    sa.ForeignKeyConstraint(['status_id'], ['process_status_detail.status_id'],),
                    sa.PrimaryKeyConstraint('status_id'))
    # yapf: enable
    op.create_index('result_status_result_idx', 'result_status', ['result_code'], unique=False)

    op.execute('''
        CREATE OR REPLACE VIEW process_result_view AS
        SELECT psd.status_id, psd.fwan_process_id, psd.file_id,
               psd.component_id, psd.status, psd.status_date,
               rs.result_code, ed.details AS error_details
        FROM process_status_detail psd
             LEFT OUTER JOIN result_status rs ON psd.status_id = rs.status_id
             LEFT OUTER JOIN error_detail ed ON psd.status_id = ed.status_id
        WHERE psd.status IN ('DONE', 'ERROR', 'DUPLICATE')
    ''')


def downgrade():
    op.execute('DROP VIEW IF EXISTS process_result_view')
    op.drop_index('result_status_result_idx', table_name='result_status')
    op.drop_table('result_status')
