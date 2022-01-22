import datetime

from sqlalchemy import and_, create_engine, not_, select
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
        # self.filters = {'child_filer': }

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
    def _child_path(session, path, **kwargs):
        path_filter = path + r'\.\d+'
        path_filter_dot = path + r'\.\d+\.'
        task = select(CommentsDB).where(and_(
            CommentsDB.path.regexp_match(path_filter),
            not_(CommentsDB.path.regexp_match(path_filter_dot))
        )).filter_by(**kwargs)
        last_sample = session.execute(task).fetchone()
        return last_sample

    @staticmethod
    def _first_level_paths(session, **kwargs):
        path_filter = r'(.*\..*)'
        task = select(CommentsDB).where(not_(
            CommentsDB.path.regexp_match(path_filter)
        )).filter_by(**kwargs)
        last_sample = session.execute(task)
        return last_sample

    def _get_id(self, session, table, **kwargs):
        sample_id = self._select_one(session, table, **kwargs)
        if not sample_id:
            self._add_to_bd(session, table, **kwargs)
            sample_id = self._select_one(session, table, **kwargs)

        return sample_id[0].id

    def _create_child_path(self, session, parent_id):
        parent = self._select_one(session, CommentsDB, id=parent_id)[0]
        last_child = self._child_path(session, parent.path, last=True)
        if last_child:
            path = self.path_pr.next_child_path(last_child[0].path,
                                                first=False)
        else:
            path = self.path_pr.next_child_path(parent.path)
        return path, last_child, parent.url_id

    def _create_first_level_path(self, session):
        last_comment = self._first_level_paths(session, last=True)
        last_comment = last_comment.fetchone()
        if last_comment:
            path = self.path_pr.next_path(last_comment[0].path, first=False)
        else:
            path = self.path_pr.next_path()
        return path, last_comment

    @session_decorator
    def _put_comment(
            self, session, parent_id, url, user, comment, last=True
    ):
        user_id = self._get_id(session, UserDB, user=user)
        if parent_id:
            path, last_comment, url_id = self._create_child_path(session,
                                                                 parent_id)
        else:
            path, last_comment = self._create_first_level_path(session)
            url_id = self._get_id(session, URLsDB, url=url)
        current_time = datetime.datetime.now()
        kwargs = {'path': path,
                  'user_id': user_id,
                  'url_id': url_id,
                  'comment': comment,
                  'date': current_time,
                  'last': True}

        comment = CommentsDB(**kwargs)
        session.add(comment)
        if last_comment:
            last_comment[0].last = False
        session.commit()

    @session_decorator
    def _get_url_comments(self, session, url):
        url_instance = self._select_one(session, URLsDB, url=url)
        if url_instance:
            comments = self._first_level_paths(session,
                                               url_id=url_instance[0].id)
            return comments.fetchall()


if __name__ == '__main__':
    engine = create_engine('sqlite:///:memory:')
    create_models(engine)
    db_client = DBClient(engine)
    db_client._put_comment(1, 'url_1', 'Luke', 'SkyWalker?')
    db_client._put_comment(None, 'url_3', 'Luke', 'SkyWalker?')
    db_client._put_comment(3, 'url_2', 'Dart', 'ALALA')
    db_client._put_comment(2, 'url_2', 'Dart', 'StarKiller')
    db_client._put_comment(1, 'url_1', 'Jaka', 'StarLord')

    comments = db_client._get_url_comments('url_2')

    print("URL first level")
    for comment in comments:
        print(comment[0])

    print_tables(engine)
