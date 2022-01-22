import datetime

from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from db_table import CommentsDB, URLsDB, UserDB
from materialized_path import PathProcessor
from tester import create_models, print_tables


def session_decorator(func):
    """Decorator to perform database session"""
    def wrapper(self, *args, **kwargs):
        with Session(self._engine) as session:
            result = func(self, session, *args, **kwargs)
        return result
    return wrapper


class DBClient:

    def __init__(self, engine: Engine):
        self._engine = engine
        self.path_pr = PathProcessor()

    @staticmethod
    def _select_one(session, table, **kwargs):
        task = select(table).filter_by(**kwargs)
        result = session.execute(task)
        return result.fetchone()

    @staticmethod
    def _add_to_bd(session, table, **kwargs):
        sample = table(**kwargs)
        session.add(sample)
        session.commit()

    @staticmethod
    def _select_last_path(session, path_filter):
        task = select(CommentsDB).where(
            CommentsDB.path.regexp_match(path_filter)
        ).filter_by(last=True)
        last_sample = session.execute(task).fetchone()

        return last_sample

    def _get_id(self, session, table, **kwargs):
        sample_id = self._select_one(session, table, **kwargs)
        if not sample_id:
            self._add_to_bd(session, table, **kwargs)
            sample_id = self._select_one(session, table, **kwargs)

        return sample_id[0].id

    def _create_path(self, session, parent_id):
        if parent_id:
            parent = self._select_one(session, CommentsDB, id=parent_id)[0]
            path_filter = parent.path + r'.\d+'
            last_child = self._select_last_path(session, path_filter)
            if last_child:
                path = self.path_pr.get_next_child_path(last_child[0].path,
                                                        first=False)
            else:
                path = self.path_pr.get_next_child_path(parent.path)
        else:
            path_filter = r'\d+'
            last_comment = self._select_last_path(session, path_filter)
            if last_comment:
                path = self.path_pr.get_next_path(last_comment[0].path,
                                                  first=False)
            else:
                path = self.path_pr.get_next_path()

        return path

    @session_decorator
    def _put_comment(
            self, session, parent_id, url, user, comment, last=True
    ):
        user_id = self._get_id(session, UserDB, user=user)
        url_id = self._get_id(session, URLsDB, url=url)
        path = self._create_path(session, parent_id)
        print(path)
        current_time = datetime.datetime.now()
        kwargs = {'path': path,
                  'user_id': user_id,
                  'url_id': url_id,
                  'comment': comment,
                  'date': current_time,
                  'last': True}

        comment = CommentsDB(**kwargs)
        print(comment)
        session.add(comment)
        session.commit()


if __name__ == '__main__':
    engine = create_engine('sqlite:///:memory:')
    create_models(engine)
    db_client = DBClient(engine)
    db_client._put_comment(1, 'url_1', 'Luke', 'SkyWalker?')
    print_tables(engine)
