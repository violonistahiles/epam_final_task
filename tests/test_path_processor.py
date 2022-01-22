from materialized_path import PathProcessor


def test_child_path_creation_when_child_exists():
    """
    Testing path incrementation when comment is adding
    to existing comment with child comments
    """
    dummy_path = '1.45.100'
    correct_path = '1.45.101'

    test_result = PathProcessor.get_next_child_path(dummy_path, first=False)

    assert test_result == correct_path


def test_child_path_creation_when_child_is_not_exists():
    """
    Testing path incrementation when comment is adding
    to existing comment without child comments
    """
    dummy_path = '1.45.100'
    correct_path = '1.45.100.1'

    test_result = PathProcessor.get_next_child_path(dummy_path)

    assert test_result == correct_path


def test_path_creation_when_there_is_no_comments():
    """Testing path incrementation when comment is first in the table"""
    correct_path = '1'

    test_result = PathProcessor.get_next_path()

    assert test_result == correct_path


def test_path_creation_when_comments_are_exists():
    """Testing path incrementation when comment is first in the table"""
    dummy_path = '4'
    correct_path = '5'

    test_result = PathProcessor.get_next_path(dummy_path, first=False)

    assert test_result == correct_path
