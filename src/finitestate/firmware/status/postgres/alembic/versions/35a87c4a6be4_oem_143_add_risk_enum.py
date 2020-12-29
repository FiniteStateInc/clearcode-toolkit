"""OEM-143 add RISK enum

Revision ID: 35a87c4a6be4
Revises: 1d1fe669d84b
Create Date: 2020-03-05 14:18:28.973881

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import UUID


old_options = ('ANALYSIS',)
new_options = ('ANALYSIS', 'RISK')

old_type = sa.Enum(*old_options, name='activestage')
new_type = sa.Enum(*new_options, name='activestage')
tmp_type = sa.Enum(*new_options, name='_activestage')
downgrade_tmp_type = sa.Enum(*old_options, name='_activestage')

ap = sa.sql.table('active_process',
    sa.Column('fwan_process_id', UUID, primary_key=True),
    sa.Column('file_id', sa.Text, primary_key=True),
    sa.Column('active_stage', new_type, nullable=False),
    sa.Column('active_start_date', sa.DateTime(True), nullable=False))


# revision identifiers, used by Alembic.
revision = '35a87c4a6be4'
down_revision = '1d1fe669d84b'
branch_labels = None
depends_on = None


def upgrade():
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute('ALTER TABLE active_process ALTER COLUMN active_stage TYPE _activestage USING active_stage::text::_activestage')
    old_type.drop(op.get_bind(), checkfirst=False)

    new_type.create(op.get_bind(), checkfirst=False)
    op.execute('ALTER TABLE active_process ALTER COLUMN active_stage TYPE activestage USING active_stage::text::activestage')
    tmp_type.drop(op.get_bind(), checkfirst=False)


def downgrade():
    op.execute(ap.update().where(ap.c.active_stage=='RISK').values(active_stage='ANALYSIS'))
    downgrade_tmp_type.create(op.get_bind(), checkfirst=False)

    op.execute('ALTER TABLE active_process ALTER COLUMN active_stage TYPE _activestage USING active_stage::text::_activestage')
    new_type.drop(op.get_bind(), checkfirst=False)

    old_type.create(op.get_bind(), checkfirst=False)
    op.execute('ALTER TABLE active_process ALTER COLUMN active_stage TYPE activestage USING active_stage::text::activestage')
    downgrade_tmp_type.drop(op.get_bind(), checkfirst=False)
