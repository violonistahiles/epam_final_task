from typing import Callable, Dict, List


def result_decorator(func: Callable) -> Callable:
    """
    Decorator to exclude comments processing if it is already dictionary type
    :param func: Processing function
    :type func: Callable
    :return: Decorated function
    :rtype: Callable
    """
    def wrapper(comments, *args, **kwargs):
        """Return comments if it is already dict"""
        if isinstance(comments, dict):
            return comments
        result = func(comments, *args, **kwargs)

        return result
    return wrapper


class PathWorker:
    """Service class to perform operations with materialized paths"""
    def __init__(self):
        self.filters = {'inherits_one_level': r'\..*\.',
                        'first_level': r'(.*\..*)'}

    @staticmethod
    def decode(path: str):
        """Method to decode path node"""
        pass

    @staticmethod
    def encode(path: str):
        """Method to encode path node"""
        pass

    @staticmethod
    def next_child_path(path: str, first: bool = True) -> str:
        """
        Create next child path
        :param path: Parent path
        :type path: str
        :param first: Flag if this comment is first inheritor in this node
        :type first: bool
        :return: Next generated path
        :rtype: str
        """
        if first:
            return path + '.1'
        else:
            dot_index = path.rfind('.')
            previous_number = path[dot_index+1:]
            return path[:dot_index+1] + f'{(int(previous_number) + 1)}'

    @staticmethod
    def next_path(path: str = '1', first: bool = True) -> str:
        """
        Create next first level path
        :param path: Precious first level path if it is exists
        :type path: str
        :param first: Flag if this comment is first in the comments table
        :type first: bool
        :return: Next generated path
        :rtype: str
        """
        return path if first else str(int(path) + 1)

    @staticmethod
    @result_decorator
    def cut_paths(comments: List) -> List:
        """
        Remove first level path from comments path (1.1.2 -> 1.2)
        :param comments: List of comments data
        :type comments: List
        :return: List of comments with modified paths
        :rtype: List
        """
        # Zero index is related to materialized path
        result = []
        for i, comment in enumerate(comments):
            path = comment[0]
            path = path[path.find('.')+1:]
            result.append([path, *comments[i][1:]])

        return result

    @staticmethod
    @result_decorator
    def create_sorted_dict(comments: List, keys: List) -> Dict:
        """
        Create nested dictionary with hierarchical comments data
        :param comments: List of comments data
        :type comments: List
        :param keys: List of keys for creating dictionary. Corresponds to
                     one comment elements after zero index
        :type keys: List
        :return: Nested dictionary
        :rtype: Dict
        """
        result = dict()
        # Sort comments by its path length and path order
        # Zero index is related to materialized path
        sorted_comments = sorted(comments,
                                 key=lambda x: (len(x[0].split('.')), x[0]))
        # Create dictionary for fixing comments order in final dictionary
        swap_dict = {}
        for ind, comment in enumerate(sorted_comments, start=1):
            if len(comment[0].split('.')) == 1:
                swap_dict.update({comment[0]: str(ind)})
            else:
                break
        # Place comments in dictionary in hierarchical order
        # 1: comment_1
        #      1: comment_1.1
        #          1: comment_1.1.1
        #      2: comment_1.2
        for comment in sorted_comments:
            info = {key: value for key, value in zip(keys, comment[1:])}
            paths = comment[0].split('.')
            if len(paths) == 1:
                result[swap_dict[paths[0]]] = {**info, 'comments': dict()}
            else:
                tmp_dict = result[swap_dict[paths[0]]]['comments']
                for path in paths[1:-1]:
                    tmp_dict = tmp_dict[path]['comments']
                tmp_dict.update({paths[-1]: {**info, 'comments': dict()}})

        return result

    @staticmethod
    @result_decorator
    def create_dict(comments: List, keys: List) -> Dict:
        """
        Create unordered dictionary with comments
        :param comments: List of comments data
        :type comments: List
        :param keys: List of keys for creating dictionary. Corresponds to
                     one comment elements after zero index
        :type keys: List
        :return: Dictionary with comments
        :rtype: Dict
        """
        result = dict()
        for ind, comment in enumerate(comments, start=1):
            info = {key: value for key, value in zip(keys, comment[1:])}
            result.update({str(ind): info})

        return result
