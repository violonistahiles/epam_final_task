import datetime

from sqlalchemy import and_, create_engine, not_
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

from db_table import CommentsDB, URLsDB, UserDB
from path_worker import PathProcessor
from tester import create_models, print_tables


def session_decorator(func):
    """Decorator to perform database session"""
    def wrapper(self, *args, **kwargs):
        with Session(self._engine) as session:
            result = func(self, session, *args, **kwargs)
        return result
    return wrapper


def comments_generator(comments):
    pass


class DBClient:

    def __init__(self, engine: Engine):
        self._engine = engine
        self.path_pr = PathProcessor()
        self.filters = {'child': r'\.\d+',
                        'child_dot': r'\.\d+\.',
                        'first_level': r'(.*\..*)',
                        }

    @staticmethod
    def _select(session, table, **kwargs):
        return session.query(table).filter_by(**kwargs)

    @staticmethod
    def _add_to_bd(session, table, **kwargs):
        sample = table(**kwargs)
        session.add(sample)
        session.commit()

    def _get_id(self, session, table, **kwargs):
        try:
            sample_id = self._select(session, table, **kwargs).one()
        except NoResultFound:
            self._add_to_bd(session, table, **kwargs)
            sample_id = self._select(session, table, **kwargs).one()

        return sample_id.id

    def _child_path(self, session, path, **kwargs):
        path_filter = path + self.filters['child']
        path_filter_dot = path + self.filters['child_dot']

        try:
            last_sample = session.query(CommentsDB).filter(and_(
                CommentsDB.path.regexp_match(path_filter),
                not_(CommentsDB.path.regexp_match(path_filter_dot))
            )).filter_by(**kwargs).one()
        except NoResultFound:
            last_sample = None

        return last_sample

    def _first_level_paths(self, session, **kwargs):
        try:
            last_sample = session.query(CommentsDB).filter(not_(
                CommentsDB.path.regexp_match(self.filters['first_level'])
            )).filter_by(**kwargs).one()
        except NoResultFound:
            last_sample = None

        return last_sample

    def _create_child_path(self, session, parent_id):
        parent = self._select(session, CommentsDB, id=parent_id).one()
        last_child = self._child_path(session, parent.path, last=True)
        if last_child:
            path = self.path_pr.next_child_path(last_child.path,
                                                first=False)
        else:
            path = self.path_pr.next_child_path(parent.path)
        return path, last_child, parent.url_id

    def _create_first_level_path(self, session):
        last_comment = self._first_level_paths(session, last=True)
        if last_comment:
            path = self.path_pr.next_path(last_comment.path, first=False)
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
            last_comment.last = False
        session.commit()

        return self._get_id(session, CommentsDB, path=path)

    @session_decorator
    def _get_url_comments(self, session, url):
        try:
            url_instance = self._select(session, URLsDB, url=url).one()
        except NoResultFound:
            return

        query = session.query(
            CommentsDB.path,
            UserDB.user,
            CommentsDB.comment,
            CommentsDB.date
        )
        join_query = query.join(UserDB)
        return join_query.filter(CommentsDB.url_id == url_instance.id).all()

    def _get_comment_inheritors(self, session, comment_id):
        parent = self._select(session, CommentsDB, id=comment_id).one()
        query = session.query(
            CommentsDB.path,
            UserDB.user,
            CommentsDB.comment,
            CommentsDB.date
        )
        join_query = query.join(UserDB)
        return join_query.filter(CommentsDB.path.startswith(parent.path)).all()

    def _ger_url_inheritors(self, session, url):
        url_id = self._get_id(session, URLsDB, url=url)
        query = session.query(
            CommentsDB.path,
            UserDB.user,
            CommentsDB.comment,
            CommentsDB.date
        )
        join_query = query.join(UserDB)
        return join_query.filter(CommentsDB.url_id == url_id).all()

    @session_decorator
    def _get_inheritors(self, session, comment_id=None, url=None):
        if comment_id:
            tree = self._get_comment_inheritors(session, comment_id)
        elif url:
            tree = self._ger_url_inheritors(session, url)
        else:
            return None

        comments_dict = self.path_pr.create_dict(tree)
        return comments_dict


if __name__ == '__main__':
    engine = create_engine('sqlite:///:memory:')
    create_models(engine)
    db_client = DBClient(engine)
    _ = db_client._put_comment(1, 'url_1', 'Luke', 'SkyWalker?')
    _ = db_client._put_comment(None, 'url_3', 'Luke', 'SkyWalker?')
    _ = db_client._put_comment(3, 'url_2', 'Dart', 'ALALA')
    _ = db_client._put_comment(2, 'url_2', 'Dart', 'StarKiller')
    _ = db_client._put_comment(1, 'url_1', 'Jaka', 'StarLord')

    comments = db_client._get_url_comments('url_1')
    comments_dict = db_client._get_inheritors(comment_id=1, url='')

    print("URL first level")
    for comment in comments:
        print(comment)

    print(comments_dict)

    print_tables(engine)
