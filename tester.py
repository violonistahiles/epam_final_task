from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from db_table import Base, CommentsDB, URLsDB, UserDB


def create_models(engine):

    Base.metadata.create_all(engine)

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
                           date=145.0,
                           last=True)

    comment_4 = CommentsDB(path='2', user_id=2, url_id=2,
                           comment='first comment',
                           date=150.0,
                           last=True)

    comment_5 = CommentsDB(path='1.1.1', user_id=2, url_id=1,
                           comment='1.1.1 comment',
                           date=150.0,
                           last=True)

    with Session(engine) as session:
        session.add(user_first)
        session.add(user_second)
        session.add(url_first)
        session.add(url_second)
        session.add(comment_1)
        session.add(comment_2)
        session.add(comment_3)
        session.add(comment_4)
        session.add(comment_5)
        session.commit()

    return engine


def print_tables(engine):
    with Session(engine) as session:
        result = session.execute(select(UserDB))
        print('Users')
        for res in result:
            print(res)

        result = session.execute(select(URLsDB))
        print('URLs')
        for res in result:
            print(res)

        result = session.execute(select(CommentsDB))
        print('Comments')
        for res in result:
            print(res)


if __name__ == '__main__':
    engine = create_engine('sqlite:///:memory:')
    create_models(engine)
    print_tables(engine)
