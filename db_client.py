import csv
import datetime
import json
import os
from typing import Any, Callable, Dict, Union

from sqlalchemy import and_, create_engine, not_
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Query, Session

from db_table import CommentsDB, URLsDB, UserDB
from path_worker import PathProcessor
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


class QueryHelper:
    """Service class to handle basic operations with database query"""
    @staticmethod
    def _filter_by_time(
            query: Query,
            start: Union[bool, float] = None,
            end: Union[bool, float] = None
    ) -> Query:
        """
        Filter data by time interval

        :param query: Query to database
        :type query: Query
        :param start: Optional, if specified: start of time interval
        :type start: Union[bool, float]
        :param end: Optional, if specified: end of time interval
        :type end: Union[bool, float]
        :return: Modified query
        :rtype: Query
        """

        start = start if start else 0
        end = end if end else datetime.datetime.now().timestamp()
        query = query.filter(and_(CommentsDB.date > start,
                                  CommentsDB.date < end))
        return query

    @staticmethod
    def _parse_parameter(key: str, **kwargs: Any) -> Union[Any, None]:
        """
        Parse value from keyword arguments
        :param key: Key which value to parse from kwargs
        :type key: str
        :param kwargs: Keyword arguments
        :type kwargs: Any
        :return: Parsed value if it is exists, else None
        :rtype: Union[Any, None]
        """
        return kwargs[key] if key in kwargs else None

    @staticmethod
    def get_base_query(session: Session) -> Query:
        """
        Create basic query to database
        :param session: Manages persistence operations for ORM-mapped objects
        :type session: Session
        :return: Query
        :rtype: Query
        """
        query = session.query(
            CommentsDB.path,
            CommentsDB.id,
            UserDB.user,
            CommentsDB.comment,
            CommentsDB.date
        ).join(UserDB)
        return query

    @staticmethod
    def check_query(query: Query) -> Union[Query, None]:
        """
        Check if query return nothing
        :param query: Query to database
        :type query: Query
        :return: Query if result is not empty, else None
        :rtype: Union[Query, None]
        """
        try:
            result = query.one()
        except NoResultFound:
            return
        return result

    def modify_data(self, query, **kwargs):
        """
        Apply selected filters for query
        :param query: Query to database
        :type query: Query
        :param kwargs: Keyword arguments containing filter parameters
        :type kwargs: Any
        :return: Modified query
        :rtype: Query
        """
        last = self._parse_parameter('last', **kwargs)
        do_sort = self._parse_parameter('do_sort', **kwargs)
        start = self._parse_parameter('start', **kwargs)
        end = self._parse_parameter('end', **kwargs)

        if last:
            query = query.filter_by(last=True)
        if start or end:
            query = self._filter_by_time(query, start=start, end=end)
        if do_sort:
            query = query.order_by(CommentsDB.date)

        return query


class DBClient:

    def __init__(self, engine: Engine):
        self._engine = engine
        self.path_pr = PathProcessor()
        self._qhelper = QueryHelper()
        self.keys = ['comment_id', 'user', 'comment', 'date']

    @staticmethod
    def _save_json(data_dict, data_path):
        with open(data_path, 'w') as f:
            json.dump(data_dict, f)

    def _save_csv(self, data: Dict, file_path):
        """
        Save single data row to report.csv
        :param data: Dictionary with values for single line in .csv file
        :type data: Dict
        :param file_path: path to .csv file
        :type file_path: str
        """
        with open(file_path, 'w', newline='') as fi:
            report = csv.writer(fi)
            report.writerow(self.keys)
            for ind in data:
                data_list = [data[ind][key] for key in self.keys]
                data_list[-1] = datetime.datetime.fromtimestamp(data_list[-1])
                report.writerow(data_list)

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
        path_filter = path + self.path_pr.filters['child']
        path_filter_dot = path + self.path_pr.filters['child_dot']

        query = session.query(CommentsDB).filter(and_(
            CommentsDB.path.regexp_match(path_filter),
            not_(CommentsDB.path.regexp_match(path_filter_dot))))
        query = self._qhelper.modify_data(query, **kwargs)

        return query

    def _first_level_paths(self, session, **kwargs):
        query = session.query(CommentsDB).filter(not_(
            CommentsDB.path.regexp_match(
                self.path_pr.filters['first_level'])))
        query = self._qhelper.modify_data(query, **kwargs)

        return query

    def _create_child_path(self, session, parent_path):
        last_child = self._child_path(session, parent_path, last=True)
        last_child = self._qhelper.check_query(last_child)
        if last_child:
            path = self.path_pr.next_child_path(last_child.path, first=False)
            last_child.last = False
        else:
            path = self.path_pr.next_child_path(parent_path)
        return path

    def _create_first_level_path(self, session):
        last_comment = self._first_level_paths(session, last=True)
        last_comment = self._qhelper.check_query(last_comment)
        if last_comment:
            path = self.path_pr.next_path(last_comment.path, first=False)
            last_comment.last = False
        else:
            path = self.path_pr.next_path()
        return path

    @session_decorator
    def _put_comment(
            self, session, parent_id, url, user, comment, last=True
    ):
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
        parent = self._select(session, CommentsDB, id=comment_id).one()
        query = self._qhelper.get_base_query(session)
        result = query.filter(CommentsDB.path.startswith(parent.path)).all()
        return result

    @session_decorator
    def _ger_url_inheritors(
            self, session, url, first_level=True, **kwargs
    ):
        url_id = self._get_id(session, URLsDB, url=url)
        query = self._qhelper.get_base_query(session)
        query = query.filter(CommentsDB.url_id == url_id)
        query = self._qhelper.modify_data(query, **kwargs)
        if first_level:
            query = query.filter(not_(CommentsDB.path.regexp_match(
                self.path_pr.filters['first_level'])))

        return query.all()

    @session_decorator
    def _get_user_comments(self, session, user, **kwargs):
        user_id = self._get_id(session, UserDB, user=user)
        query = self._qhelper.get_base_query(session)
        query = query.filter(CommentsDB.user_id == user_id)
        query = self._qhelper.modify_data(query, **kwargs)
        return query.all()

    def _get_url_comments(self, url, **kwargs):
        result = self._ger_url_inheritors(url, **kwargs)
        comments_dict = self.path_pr.create_sorted_dict(result, self.keys)
        return comments_dict

    def _get_comment_tree(
            self, url, comment_id=None, first_level=True, **kwargs
    ):
        if comment_id:
            tree = self._get_comment_inheritors(comment_id)
        else:
            tree = self._ger_url_inheritors(url, first_level, **kwargs)
        comments_dict = self.path_pr.create_sorted_dict(tree, self.keys)
        return comments_dict

    def _get_user_history(self, user, **kwargs):
        result = self._get_user_comments(user, **kwargs)
        comments_dict = self.path_pr.create_dict(result, self.keys)
        return comments_dict

    def _save_results(self, file_path, user=None, url=None, **kwargs):
        if user:
            result_dict = self._get_user_history(user, **kwargs)
        else:
            result = self._ger_url_inheritors(url, first_level=False, **kwargs)
            result_dict = self.path_pr.create_dict(result, self.keys)

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
    comments_dict = db_client._get_comment_tree(url='url_1', comment_id=None,
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
    print(datetime.datetime.fromtimestamp(comments_dict['1']['date']))

    print_tables(engine)
