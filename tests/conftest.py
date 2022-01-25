import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from db_table import Base, CommentsDB, URLsDB, UserDB


@pytest.fixture
def database():
    """Create fake database for tests"""
    engine = create_engine('sqlite+pysqlite:///:memory:')
    Base.metadata.create_all(engine)

    user_first = UserDB(user='user_1')
    user_second = UserDB(user='user_2')

    url_first = URLsDB(url='url_1')
    url_second = URLsDB(url='url_2')

    comment_1 = CommentsDB(path='1', user_id=1, url_id=1,
                           comment='first comment',
                           date=5.0,
                           last=False)

    comment_2 = CommentsDB(path='1.1', user_id=1, url_id=1,
                           comment='1.1 comment',
                           date=15.0,
                           last=False)

    comment_3 = CommentsDB(path='1.2', user_id=2, url_id=1,
                           comment='1.2 comment',
                           date=20.0,
                           last=True)

    comment_4 = CommentsDB(path='2', user_id=2, url_id=2,
                           comment='first comment',
                           date=20.0,
                           last=False)

    comment_5 = CommentsDB(path='1.1.1', user_id=2, url_id=1,
                           comment='1.1.1 comment',
                           date=25.0,
                           last=True)

    comment_6 = CommentsDB(path='3', user_id=2, url_id=1,
                           comment='first comment',
                           date=20.0,
                           last=True)

    with Session(engine) as session:
        session.add_all([user_first, user_second, url_first, url_second,
                         comment_1, comment_2, comment_3, comment_4,
                         comment_5, comment_6])
        session.commit()

    return engine
