from sqlalchemy.orm import Session

from db_client import DBClient
from db_table import CommentsDB, UserDB


def test_get_id_when_id_is_exists(database):
    """Testing get_id method when element in table exists"""
    request_data = {'user': 'user_2'}
    table = UserDB
    db_client = DBClient(database)
    correct_id = 2

    with Session(database) as session:
        test_id = db_client._get_id(session, table, **request_data)

    assert test_id == correct_id


def test_get_id_when_id_is_not_exists(database):
    """Testing get_id method when element in table don't exists"""
    request_data = {'user': 'user_3'}
    table = UserDB
    db_client = DBClient(database)
    correct_id = 3

    with Session(database) as session:
        test_id = db_client._get_id(session, table, **request_data)

    assert test_id == correct_id


def test_create_child_path_when_it_is_first_child(database):
    """Testing child path creation when new comment is first inheritor"""
    parent_path = '1.2'
    db_client = DBClient(database)
    correct_path = '1.2.1'

    with Session(database) as session:
        test_path = db_client._create_child_path(session, parent_path)

    assert test_path == correct_path


def test_create_child_path(database):
    """Testing child path creation when inheritors are already exists"""
    parent_path = '1.1'
    db_client = DBClient(database)
    correct_path = '1.1.2'

    with Session(database) as session:
        test_path = db_client._create_child_path(session, parent_path)

    assert test_path == correct_path


def test_create_first_level_path_when_comment_is_first(database):
    """Testing first level path creation when it is first comment"""

    def fake_check_query(*args):
        return

    db_client = DBClient(database)
    db_client._qhelper.check_query = fake_check_query
    correct_path = '1'

    with Session(database) as session:
        test_path = db_client._create_first_level_path(session)

    assert test_path == correct_path


def test_create_first_level_path_when_comments_are_exist(database):
    """Testing first level path creation when comments are already exist"""
    db_client = DBClient(database)
    correct_path = '4'

    with Session(database) as session:
        test_path = db_client._create_first_level_path(session)

    assert test_path == correct_path


def test_get_table_data(database):
    """Testing get_table_data method works correctly"""
    table = 'urls'
    db_client = DBClient(database)
    correct_result = [{'id': '1', 'url': 'url_1'},
                      {'id': '2', 'url': 'url_2'}]

    test_result = db_client.get_table_data(table)

    assert test_result == correct_result


def test_add_comment(database):
    """Testing add_comment method works correctly"""
    parent_id = 1
    url = 'url_1'
    user = 'user_3'
    comment = 'dummy_comment'
    db_client = DBClient(database)
    correct_result = 7

    test_result = db_client.add_comment(parent_id, url, user, comment)

    with Session(database) as session:
        previous_comment = db_client._select(session, CommentsDB, id=3).one()

    assert test_result == correct_result
    assert not previous_comment.last
