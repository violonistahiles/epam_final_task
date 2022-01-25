import datetime
from typing import Any, Dict, Union

from sqlalchemy import and_, not_
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Query, Session

from db_table import CommentsDB, UserDB


class QueryHelper:
    """Service class to handle basic operations with database query"""
    def __init__(self, filters: Dict):
        """
        :param filters: Filters to find materialized path patterns
        :type filters: Dict
        """
        self._path_filters = filters
        self.keys = ['comment_id', 'user', 'comment', 'date']

    @staticmethod
    def _filter_by_time(
            query: Query,
            start: Union[bool, float] = None,
            end: Union[bool, float] = None
    ) -> Query:
        """
        Filter data by time interval
        :param query: Query to database
        :type query: sqlalchemy.orm.Query
        :param start: Optional, if specified: start of time interval
        :type start: Union[bool, float]
        :param end: Optional, if specified: end of time interval
        :type end: Union[bool, float]
        :return: Modified query
        :rtype: sqlalchemy.orm.Query
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
        :type session: sqlalchemy.orm.Session
        :return: Query
        :rtype: sqlalchemy.orm.Query
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
        :type query: sqlalchemy.orm.Query
        :return: Query if result is not empty, else None
        :rtype: Union[sqlalchemy.orm.Query, None]
        """
        try:
            result = query.one()
        except NoResultFound:
            return
        return result

    @staticmethod
    def child_path(query: Query, path: str) -> Query:
        """
        Get nested comments from specified parent path
        :param query: Query to database
        :type query: sqlalchemy.orm.Query
        :param path: Parent path in comments table
        :type path: str
        :return: Query
        :rtype: sqlalchemy.orm.Query
        """
        path_filter = path + '.'
        query = query.filter(CommentsDB.path.startswith(path_filter))
        return query

    def one_level_child_path(self, query: Query, path: str) -> Query:
        """
        Get only one level depth comments form specified parent path
        :param query: Query to database
        :type query: sqlalchemy.orm.Query
        :param path: Parent path in comments table
        :type path: str
        :return: Query
        :rtype: sqlalchemy.orm.Query
        """
        path_filter = path + self._path_filters['inherits_one_level']
        query = self.child_path(query, path)
        query = query.filter(not_(CommentsDB.path.regexp_match(path_filter)))
        return query

    def first_level_path(self, query: Query) -> Query:
        """
        Get only first level comment from comments table
        :param query: Query to database
        :type query: sqlalchemy.orm.Query
        :return: Query
        :rtype: sqlalchemy.orm.Query
        """
        path_filter = self._path_filters['first_level']
        query = query.filter(not_(CommentsDB.path.regexp_match(path_filter)))

        return query

    def modify_data(self, query: Query, **kwargs: Any) -> Query:
        """
        Apply selected filters for query
        :param query: Query to database
        :type query: sqlalchemy.orm.Query
        :param kwargs: Keyword arguments containing filter parameters:
            :start: (float) Start of time interval for filtering data
            :end: (float) End of time interval for filtering data
            :last: (bool) Get only last actual comments
            :do_sort: (bool) Sort data by time
        :type kwargs: Any
        :return: Modified query
        :rtype: sqlalchemy.orm.Query
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
