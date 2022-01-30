import pytest
from sqlalchemy.orm import Session

from db_backend.db_table import CommentsDB
from db_backend.path_worker import PathWorker
from db_backend.query_helper import QueryHelper


@pytest.mark.usefixtures("database")
def test_filter_by_time_when_start_defined(database):
    start = 20.
    qhelper = QueryHelper({'filters': ''})
    correct_result = 5

    with Session(database) as session:
        query = session.query(CommentsDB)
        query = qhelper._filter_by_time(query, start=start)
        result = query.all()

    assert result[0].id == correct_result


@pytest.mark.usefixtures("database")
def test_filter_by_time_when_start_and_end_defined(database):
    start = 5.
    end = 20.
    qhelper = QueryHelper({'filters': ''})
    correct_result = 1

    with Session(database) as session:
        query = session.query(CommentsDB)
        query = qhelper._filter_by_time(query, start=start, end=end)
        result = query.all()

    assert result[0].id == correct_result


@pytest.mark.usefixtures("database")
def test_child_path(database):
    path = '1'
    qhelper = QueryHelper({'filters': ''})
    correct_result = [2, 3, 5]

    with Session(database) as session:
        query = session.query(CommentsDB)
        query = qhelper.child_path(query, path)
        results = query.all()

    for i, result in enumerate(results):
        assert result.id == correct_result[i]


@pytest.mark.usefixtures("database")
def test_one_level_child_path(database):
    path = '1'
    phelper = PathWorker()
    qhelper = QueryHelper(phelper.filters)
    correct_result = [2, 3]

    with Session(database) as session:
        query = session.query(CommentsDB)
        query = qhelper.one_level_child_path(query, path)
        results = query.all()

    for i, result in enumerate(results):
        assert result.id == correct_result[i]


@pytest.mark.usefixtures("database")
def test_first_level_path(database):
    phelper = PathWorker()
    qhelper = QueryHelper(phelper.filters)
    correct_result = [1, 4, 6]

    with Session(database) as session:
        query = session.query(CommentsDB)
        query = qhelper.first_level_path(query)
        results = query.all()

    for i, result in enumerate(results):
        assert result.id == correct_result[i]


@pytest.mark.usefixtures("database")
def test_modify_data_sort(database):
    parameters = {'do_sort': True}
    phelper = PathWorker()
    qhelper = QueryHelper(phelper.filters)
    correct_result = [4, 1, 6]

    with Session(database) as session:
        query = session.query(CommentsDB)
        query = qhelper.first_level_path(query)
        query = qhelper.modify_data(query, **parameters)
        results = query.all()

    for i, result in enumerate(results):
        assert result.id == correct_result[i]


@pytest.mark.usefixtures("database")
def test_modify_data_last(database):
    parameters = {'last': True}
    phelper = PathWorker()
    qhelper = QueryHelper(phelper.filters)
    correct_result = [3, 5, 6]

    with Session(database) as session:
        query = session.query(CommentsDB)
        query = qhelper.modify_data(query, **parameters)
        results = query.all()

    for i, result in enumerate(results):
        assert result.id == correct_result[i]
