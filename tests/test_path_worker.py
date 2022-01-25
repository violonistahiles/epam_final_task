from path_worker import PathWorker, result_decorator


def test_result_decorator():
    """Testing result_decorator"""

    @result_decorator
    def dummy_func(some_input):
        return 10

    test_input = {'a': 'b'}

    assert dummy_func(test_input) == test_input


def test_child_path_creation_when_child_exists():
    """
    Testing path incrementation when comment is adding
    to existing comment with child comments
    """
    dummy_path = '1.45.100'
    correct_path = '1.45.101'

    test_result = PathWorker.next_child_path(dummy_path, first=False)

    assert test_result == correct_path


def test_child_path_creation_when_child_is_not_exists():
    """
    Testing path incrementation when comment is adding
    to existing comment without child comments
    """
    dummy_path = '1.45.100'
    correct_path = '1.45.100.1'

    test_result = PathWorker.next_child_path(dummy_path)

    assert test_result == correct_path


def test_path_creation_when_there_is_no_comments():
    """Testing path incrementation when comment is first in the table"""
    correct_path = '1'

    test_result = PathWorker.next_path()

    assert test_result == correct_path


def test_path_creation_when_comments_are_exists():
    """Testing path incrementation when comment is first level and not first"""
    dummy_path = '4'
    correct_path = '5'

    test_result = PathWorker.next_path(dummy_path, first=False)

    assert test_result == correct_path


def test_cut_paths():
    """Testing cut_paths method works correctly"""
    test_input = [['1.1', 'a'], ['1.2', 'b'], ['1.1.1', 'c']]
    correct_output = [['1', 'a'], ['2', 'b'], ['1.1', 'c']]

    test_output = PathWorker.cut_paths(test_input)

    assert test_output == correct_output


def test_create_sorted_dict():
    """Testing create_sorted_dict method works correctly"""
    test_input = [['2', 'a'], ['1', 'b'], ['2.1', 'c']]
    correct_output = {'1': {'letter': 'b', 'comments': {}},
                      '2': {'letter': 'a', 'comments':
                            {'1': {'letter': 'c', 'comments': {}}}}}
    keys = ['letter']

    test_output = PathWorker.create_sorted_dict(test_input, keys)

    assert test_output == correct_output


def test_create_sorted_dict_with_long_nested_structure():
    """
    Testing create_sorted_dict method works correctly
    with long nested structures
    """
    test_input = [['1', 'a'], ['1.1', 'b'], ['1.1.1', 'c']]
    correct_output = {'1': {'letter': 'a', 'comments': {
                        '1': {'letter': 'b', 'comments':
                            {'1': {'letter': 'c', 'comments': {}}}}}}}

    keys = ['letter']

    test_output = PathWorker.create_sorted_dict(test_input, keys)

    assert test_output == correct_output


def test_create_sorted_dict_when_comment_is_one():
    """
    Testing create_sorted_dict method works correctly when comment is only one
    """
    test_input = [['1', 'a']]
    correct_output = {'1': {'letter': 'a', 'comments': {}}}
    keys = ['letter']

    test_output = PathWorker.create_sorted_dict(test_input, keys)

    assert test_output == correct_output


def test_create_dict():
    """Testing create_dict method works correctly"""
    test_input = [['2', 'a'], ['1', 'b'], ['2.1', 'c']]
    correct_output = {'1': {'letter': 'a'},
                      '2': {'letter': 'b'},
                      '3': {'letter': 'c'}}
    keys = ['letter']

    test_output = PathWorker.create_dict(test_input, keys)

    assert test_output == correct_output
