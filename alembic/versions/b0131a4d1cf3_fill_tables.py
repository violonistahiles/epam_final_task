"""Fill tables

Revision ID: b0131a4d1cf3
Revises: f7839842e7ee
Create Date: 2022-01-25 00:00:33.716993

"""
from sqlalchemy import orm

from alembic import op
from db_backend.db_table import CommentsDB, URLsDB, UserDB

# revision identifiers, used by Alembic.
revision = 'b0131a4d1cf3'
down_revision = 'f7839842e7ee'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    session = orm.Session(bind=bind)

    user_first = UserDB(user='Anakin')
    user_second = UserDB(user='Luke')

    url_first = URLsDB(url='url_1')
    url_second = URLsDB(url='url_2')

    comment_1 = CommentsDB(path='1', user_id=1, url_id=1,
                           comment='first comment',
                           date=150.0,
                           last=False)

    comment_2 = CommentsDB(path='1.1', user_id=1, url_id=1,
                           comment='1.1 comment',
                           date=15.0,
                           last=False)

    comment_3 = CommentsDB(path='1.2', user_id=2, url_id=1,
                           comment='1.2 comment',
                           date=14.0,
                           last=True)

    comment_4 = CommentsDB(path='2', user_id=2, url_id=2,
                           comment='second comment',
                           date=150.0,
                           last=True)

    comment_5 = CommentsDB(path='1.1.1', user_id=2, url_id=1,
                           comment='1.1.1 comment',
                           date=150.0,
                           last=True)

    session.add_all([user_first, user_second, url_first, url_second,
                     comment_1, comment_2, comment_3, comment_4, comment_5])
    session.flush()
    session.commit()


def downgrade():
    pass
