import datetime
import os
from typing import Any, Callable, Dict, List, Union

from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Query, Session

from db_table import Base, CommentsDB, URLsDB, UserDB
from path_worker import PathProcessor
from query_helper import QueryHelper
from tester import create_models, print_tables


def session_decorator(func: Callable) -> Callable:
    """
    Decorator to perform database session and close it after transaction
    :param func: Function with database transactions
    :type func: Callable
    :return: Function which decorate initial function with ORM Session
    :rtype: Callable
    """
    def wrapper(self, *args, **kwargs):
        """
        Perform initial function under ORM Session and return empty dict
        if transaction cant get result
        """
        with Session(self._engine) as session:
            try:
                result = func(self, session, *args, **kwargs)
            except NoResultFound:
                return {}
        return result

    return wrapper


class DBClient:
    """Class to perform database transaction operations"""
    def __init__(self, engine: Engine):
        """
        Initialize class
        :param engine: Object establishing connection to database
        :type engine: sqlalchemy.engine.Engine
        """
        self._engine = engine
        self._path_pr = PathProcessor()
        self._qhelper = QueryHelper(self._path_pr.filters)
        self.keys = ['comment_id', 'user', 'comment', 'date']

    @staticmethod
    def _select(session: Session, table: Base, **kwargs: Any) -> Query:
        """
        Filter data from database table
        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :param table: Database table
        :type table: sqlalchemy.orm.declarative_base
        :param kwargs: Filter parameters
        :type kwargs: Any
        :return: Query
        :rtype: sqlalchemy.orm.Query
        """
        return session.query(table).filter_by(**kwargs)

    @staticmethod
    def _add_to_bd(session: Session, table: Base, **kwargs: Any) -> None:
        """
        Add table raw instance to database
        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :param table: Database table
        :type table: sqlalchemy.orm.declarative_base
        :param kwargs: Parameters for ORM class creation
        :type kwargs: Any
        """
        sample = table(**kwargs)
        session.add(sample)
        session.commit()

    def _get_id(self, session: Session, table: Base, **kwargs: Any) -> int:
        """
        Get element ID from database table
        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :param table: Database table
        :type table: sqlalchemy.orm.declarative_base
        :param kwargs: Filter parameters
        :type kwargs: Any
        :return:
        """
        try:
            sample_id = self._select(session, table, **kwargs).one()
        except NoResultFound:
            self._add_to_bd(session, table, **kwargs)
            sample_id = self._select(session, table, **kwargs).one()
        return sample_id.id

    def _create_child_path(self, session: Session, parent_path: str) -> str:
        """
        Create next child path for existing comments node
        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :param parent_path: Comments node path
        :type parent_path: str
        :return: Path to new comment
        :rtype: str
        """
        query = session.query(CommentsDB)
        query = self._qhelper.one_level_child_path(query, parent_path)
        query = self._qhelper.modify_data(query, last=True)
        last_child = self._qhelper.check_query(query)
        if last_child:
            path = self._path_pr.next_child_path(last_child.path, first=False)
            last_child.last = False
        else:
            path = self._path_pr.next_child_path(parent_path)
        return path

    def _create_first_level_path(self, session: Session) -> str:
        """
        Create path to next first level comment
        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :return: Path to next first level comment
        :rtype: str
        """
        query = session.query(CommentsDB)
        query = self._qhelper.first_level_path(query)
        query = self._qhelper.modify_data(query, last=True)
        last_comment = self._qhelper.check_query(query)
        if last_comment:
            path = self._path_pr.next_path(last_comment.path, first=False)
            last_comment.last = False
        else:
            path = self._path_pr.next_path()
        return path

    @session_decorator
    def get_table_data(self, session, table):
        data = []
        results = session.execute(select(table))
        for result in results:
            data.append(result[0].get_dict())
        return data

    @session_decorator
    def add_comment(
            self,
            session: Session,
            parent_id: int,
            url: str,
            user: str,
            comment: str,
            last: bool = True
    ) -> int:
        """
        Add new comment to comments table in database
        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :param parent_id: ID of comment parent
        :type parent_id: int
        :param url: URL address where comments is adding
        :type url: str
        :param user: Username who is adding the comment
        :type user: str
        :param comment: Text of the comment
        :type comment: str
        :param last: Flag specifying if this comment is last on current
                     comments level
        :type last: bool
        :return: Created comment ID
        :rtype: int
        """
        print(parent_id, url)
        user_id = self._get_id(session, UserDB, user=user)
        if parent_id:
            parent = self._select(session, CommentsDB, id=parent_id).one()
            path = self._create_child_path(session, parent.path)
            url_id = parent.url_id
        else:
            path, = self._create_first_level_path(session)
            url_id = self._get_id(session, URLsDB, url=url)
        current_time = datetime.datetime.now().timestamp()
        kwargs = {'path': path,
                  'user_id': user_id,
                  'url_id': url_id,
                  'comment': comment,
                  'date': current_time,
                  'last': last}

        comment_id = self._get_id(session, CommentsDB, **kwargs)
        return comment_id

    @session_decorator
    def _get_comment_inheritors(
            self, session: Session, comment_id: int
    ) -> List:
        """
        Get tree of inherited comments from specified comment
        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :param comment_id: Comment ID from comments table
        :type comment_id: int
        :return: List of inheritors
        """
        parent = self._select(session, CommentsDB, id=comment_id).one()
        query = self._qhelper.get_base_query(session)
        query = self._qhelper.child_path(query, parent.path)
        return query.all()

    @session_decorator
    def _ger_url_inheritors(
            self,
            session: Session,
            url: str,
            first_level: bool = True,
            **kwargs: Any
    ) -> List:
        """
        Get tree of inherited comments from specified URL
        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :param url: URL address
        :type url: str
        :param first_level: Flag to get only first level comments
        :type first_level: bool
        :param kwargs: Additional parameters to filter query
            :start: (float) Start of time interval for filtering data
            :end: (float) End of time interval for filtering data
            :last: (bool) Get only last actual comments
            :do_sort: (bool) Sort data by time
        :type kwargs: Any
        :return: List of inheritors
        """
        url_id = self._get_id(session, URLsDB, url=url)
        query = self._qhelper.get_base_query(session)
        query = query.filter(CommentsDB.url_id == url_id)
        query = self._qhelper.modify_data(query, **kwargs)
        if first_level:
            query = self._qhelper.first_level_path(query)

        return query.all()

    @session_decorator
    def _get_user_comments(
            self, session: Session, user: str, **kwargs: Any
    ) -> List:
        """
        Get all user comments form database
        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :param user: Username
        :type user: str
        :param kwargs: Additional parameters to filter query
            :start: (float) Start of time interval for filtering data
            :end: (float) End of time interval for filtering data
            :last: (bool) Get only last actual comments
            :do_sort: (bool) Sort data by time
        :type kwargs: Any
        :return: List of inheritors
        """
        user_id = self._get_id(session, UserDB, user=user)
        query = self._qhelper.get_base_query(session)
        query = query.filter(CommentsDB.user_id == user_id)
        query = self._qhelper.modify_data(query, **kwargs)
        return query.all()

    def get_url_comments(self, url: str, **kwargs: Any) -> Dict:
        """
        Create json for requested url inheritors
        :param url: URL Address
        :type url: str
        :param kwargs: Additional parameters to filter query
        :type kwargs: Any
        :return: Dictionary with comments
        :rtype: Dict
        """
        result = self._ger_url_inheritors(url, **kwargs)
        comments_dict = self._path_pr.create_sorted_dict(result, self.keys)
        return comments_dict

    def get_comment_tree(
            self,
            url: str,
            comment_id: Union[None, str] = None,
            first_level: bool = False,
            **kwargs: Any
    ) -> Dict:
        """
        Create json for requested url or comment inheritors
        :param url: URL address
        :type url: str
        :param comment_id: Comment ID form comments table
        :type comment_id: int
        :param first_level: Flag to get only first level comments
        :type first_level: bool
        :param kwargs: Additional parameters to filter query
        :type kwargs: Any
        :return: Dictionary with comments
        :rtype: Dict
        """
        if comment_id:
            tree = self._get_comment_inheritors(comment_id)
            tree = self._path_pr.cut_paths(tree)
        else:
            tree = self._ger_url_inheritors(url, first_level, **kwargs)
        comments_dict = self._path_pr.create_sorted_dict(tree, self.keys)
        return comments_dict

    def get_user_history(self, user: str, **kwargs: Any) -> Dict:
        """
        Create json for requested user comments history
        :param user: Username
        :type user: str
        :param kwargs: Additional parameters to filter query
        :type kwargs: Any
        :return: Dictionary with comments
        :rtype: Dict
        """
        result = self._get_user_comments(user, **kwargs)
        comments_dict = self._path_pr.create_dict(result, self.keys)
        return comments_dict

    def prepare_data_to_report(
            self,
            url: Union[None, str] = None,
            user: Union[None, str] = None,
            **kwargs: Any
    ) -> Dict:
        """
        Get user or url comments history
        :param user: Username
        :type user: str
        :param url: URL address
        :type url: str
        :param kwargs: Additional parameters to filter query
        :type kwargs: Any
        :return: Dictionary with comments
        :rtype: Dict
        """
        if user:
            result_dict = self.get_user_history(user, **kwargs)
        elif url:
            result = self._ger_url_inheritors(url, first_level=False, **kwargs)
            result_dict = self._path_pr.create_dict(result, self.keys)
        else:
            result_dict = {}

        return result_dict


if __name__ == '__main__':
    engine = create_engine('sqlite:///:memory:')
    create_models(engine)
    db_client = DBClient(engine)
    _ = db_client.add_comment(1, 'url_1', 'Luke', 'SkyWalker?')
    _ = db_client.add_comment(None, 'url_3', 'Luke', 'SkyWalker?')
    _ = db_client.add_comment(3, 'url_2', 'Dart', 'ALALA')
    _ = db_client.add_comment(2, 'url_2', 'Dart', 'StarKiller')
    _ = db_client.add_comment(1, 'url_1', 'Jaka', 'StarLord')
    _ = db_client.add_comment(None, 'url_1', 'Wuki', 'AAARRRR')

    comments = db_client.get_url_comments('url_1')
    comments_dict = db_client.get_comment_tree(url='url_1', comment_id=1)
    user_comments = db_client.get_user_history(user='Luke', do_sort=True)

    curr_path = os.getcwd()
    results_path = os.path.join(curr_path, 'build')

    csv_path = os.path.join(results_path, 'result.csv')
    db_client.prepare_data_to_report(csv_path, user='', url='url_1',
                                     do_sort=True, end=600)

    print(comments_dict)
    print(comments)
    print(user_comments)

    # print("User comments")
    # for comment in user_comments:
    #     print(user_comments[comment])
    #
    # print("URL first level")
    # for comment in comments:
    #     print(comments[comment])
    # print("Comments tree")

    print_tables(engine)
