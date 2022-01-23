import csv
import datetime
import json
import os
from typing import Any, Callable, Dict

from sqlalchemy import create_engine
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
        self._keys = ['comment_id', 'user', 'comment', 'date']

    @staticmethod
    def _save_json(data_dict: Dict, data_path: str) -> None:
        """
        Save query result to .json file
        :param data_dict: Dictionary with query result
        :type data_dict: Dict
        :param data_path: Path to .json file
        :type data_path: str
        """
        with open(data_path, 'w') as f:
            json.dump(data_dict, f)

    def _save_csv(self, data: Dict, file_path: str) -> None:
        """
        Save query result to .csv file
        :param data: Dictionary with values for single line in .csv file
        :type data: Dict
        :param file_path: path to .csv file
        :type file_path: str
        """
        with open(file_path, 'w', newline='') as fi:
            report = csv.writer(fi)
            report.writerow(self._keys)
            for ind in data:
                data_list = [data[ind][key] for key in self._keys]
                data_list[-1] = datetime.datetime.fromtimestamp(data_list[-1])
                report.writerow(data_list)

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

    def _create_child_path(self, session, parent_path):
        """

        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :param parent_path:
        :return:
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

    def _create_first_level_path(self, session):
        """

        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :return:
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
    def _put_comment(
            self, session, parent_id, url, user, comment, last=True
    ):
        """

        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :param parent_id:
        :param url:
        :param user:
        :param comment:
        :param last:
        :return:
        """
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
    def _get_comment_inheritors(self, session, comment_id):
        """

        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :param comment_id:
        :return:
        """
        parent = self._select(session, CommentsDB, id=comment_id).one()
        query = self._qhelper.get_base_query(session)
        result = self._qhelper.child_path(query, parent.path).all()
        return result

    @session_decorator
    def _ger_url_inheritors(
            self, session, url, first_level=True, **kwargs
    ):
        """

        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :param url:
        :param first_level:
        :param kwargs:
        :return:
        """
        url_id = self._get_id(session, URLsDB, url=url)
        query = self._qhelper.get_base_query(session)
        query = query.filter(CommentsDB.url_id == url_id)
        query = self._qhelper.modify_data(query, **kwargs)
        if first_level:
            query = self._qhelper.first_level_path(query)

        return query.all()

    @session_decorator
    def _get_user_comments(self, session, user, **kwargs):
        """

        :param session: Manages persistence operations for ORM-mapped objects
        :type session: sqlalchemy.orm.Session
        :param user:
        :param kwargs:
        :return:
        """
        user_id = self._get_id(session, UserDB, user=user)
        query = self._qhelper.get_base_query(session)
        query = query.filter(CommentsDB.user_id == user_id)
        query = self._qhelper.modify_data(query, **kwargs)
        return query.all()

    def _get_url_comments(self, url, **kwargs):
        result = self._ger_url_inheritors(url, **kwargs)
        comments_dict = self._path_pr.create_sorted_dict(result, self._keys)
        return comments_dict

    def _get_comment_tree(
            self, url, comment_id=None, first_level=True, **kwargs
    ):
        if comment_id:
            tree = self._get_comment_inheritors(comment_id)
            tree = self._path_pr.cut_paths(tree)
        else:
            tree = self._ger_url_inheritors(url, first_level, **kwargs)
        comments_dict = self._path_pr.create_sorted_dict(tree, self._keys)
        return comments_dict

    def _get_user_history(self, user, **kwargs):
        result = self._get_user_comments(user, **kwargs)
        comments_dict = self._path_pr.create_dict(result, self._keys)
        return comments_dict

    def _save_results(self, file_path, user=None, url=None, **kwargs):
        if user:
            result_dict = self._get_user_history(user, **kwargs)
        else:
            result = self._ger_url_inheritors(url, first_level=False, **kwargs)
            result_dict = self._path_pr.create_dict(result, self._keys)

        self._save_csv(result_dict, file_path)


if __name__ == '__main__':
    engine = create_engine('sqlite:///:memory:')
    create_models(engine)
    db_client = DBClient(engine)
    _ = db_client._put_comment(1, 'url_1', 'Luke', 'SkyWalker?')
    _ = db_client._put_comment(None, 'url_3', 'Luke', 'SkyWalker?')
    _ = db_client._put_comment(3, 'url_2', 'Dart', 'ALALA')
    _ = db_client._put_comment(2, 'url_2', 'Dart', 'StarKiller')
    _ = db_client._put_comment(1, 'url_1', 'Jaka', 'StarLord')
    _ = db_client._put_comment(None, 'url_1', 'Wuki', 'AAARRRR')

    comments = db_client._get_url_comments('url_1')
    comments_dict = db_client._get_comment_tree(url='url_1', comment_id=1,
                                                first_level=False)
    user_comments = db_client._get_user_history(user='Luke', do_sort=True)

    curr_path = os.getcwd()
    results_path = os.path.join(curr_path, 'build')

    csv_path = os.path.join(results_path, 'result.csv')
    db_client._save_results(csv_path, user='', url='url_1',
                            do_sort=True, end=600)

    print(comments_dict)

    print("User comments")
    for comment in user_comments:
        print(user_comments[comment])

    print("URL first level")
    for comment in comments:
        print(comments[comment])
    print("Comments tree")

    print_tables(engine)
