import csv
import datetime
import json
import os
from typing import Any, Callable, Dict, List, Union

from db_client import DBClient
from path_worker import PathWorker


class RequestData:
    """Class to store parsed request data"""
    def __init__(self, command, attrs):
        """
        :param command: Requested command
        :type command: str
        :param attrs: Request parameters
        :type attrs: Dict
        """
        self.command = command
        self.attrs = attrs


def parser_decorator(func: Callable) -> Callable:
    """
    Decorator to handle errors while parsing request string
    :param func: Function with database transactions
    :type func: Callable
    :return: Decorated function
    :rtype: Callable
    """
    def wrapper(self, *args, **kwargs):
        """Return 'wrong command' parsed data if error occurs"""
        try:
            result = func(self, *args, **kwargs)
        except (KeyError, ValueError, TypeError):
            return RequestData('Wrong command', '')
        return result

    return wrapper


class APIClient:
    """Class to process requests"""
    def __init__(self, engine):
        """
        :param engine: Object establishing connection to database
        :type engine: sqlalchemy.engine.Engine
        """
        curr_path = os.getcwd()
        commands_file = os.path.join(curr_path, 'commands.json')
        with open(commands_file) as fi:
            self._commands = json.load(fi)

        self.report_dir = os.path.join(curr_path, 'build')
        self._db_client = DBClient(engine)
        self._keys = self._db_client.keys
        self._pworker = PathWorker()
        self._wrong_response_message = {'Response': 'Wrong command'}

    @parser_decorator
    def _parse_request(self, request: str) -> RequestData:
        """
        Parse request data
        :param request: JSON string
        :type request: str
        :return: Class with parsed data
        :rtype: RequestData
        """
        request_dict = json.loads(request)
        command = request_dict['command']

        request_attr = {}
        for attr in self._commands[command]:
            req_attr = request_dict[attr] if attr in request_dict else None
            request_attr.update({attr: req_attr})

        return RequestData(command, request_attr)

    def _check_dir(self):
        """Check if report directory is exists otherwise create it"""
        if not os.path.exists(self.report_dir):
            os.mkdir(self.report_dir)

    def _save_json(self, data_dict: Dict) -> str:
        """
        Save query result to .json file
        :param data_dict: Dictionary with query result
        :type data_dict: Dict
        :return: Path to saved .json file
        :rtype: str
        """
        self._check_dir()
        data_path = os.path.join(self.report_dir, 'report.json')
        with open(data_path, 'w') as f:
            json.dump(data_dict, f)
        return data_path

    def _save_csv(self, data: Dict) -> str:
        """
        Save query result to .csv file
        :param data: Dictionary with values for single line in .csv file
        :type data: Dict
        :return: Path to saved .csv file
        :rtype: str
        """
        self._check_dir()
        data_path = os.path.join(self.report_dir, 'report.csv')
        with open(data_path, 'w', newline='') as fi:
            report = csv.writer(fi)
            report.writerow(self._db_client.keys)
            for ind in data:
                data_list = [data[ind][key] for key in self._db_client.keys]
                data_list[-1] = datetime.datetime.fromtimestamp(data_list[-1])
                report.writerow(data_list)
        return data_path

    def process_request(self, request: str) -> str:
        """
        Process request
        :param request: JSON string with request data
        :type request: str
        :return: JSON string with response data
        :rtype: str
        """
        data = self._parse_request(request)
        if data.command == 'add_comment':
            result = self._db_client.add_comment(**data.attrs)
        elif data.command == 'ger_url_first_level_comments':
            result = self.get_url_comments(**data.attrs)
        elif data.command == 'get_comment_tree':
            result = self.get_comment_tree(**data.attrs)
        elif data.command == 'get_user_history':
            result = self.get_user_history(**data.attrs)
        elif data.command == 'get_report':
            data_dict = self.prepare_data_to_report(**data.attrs)
            report_path = self._save_csv(data_dict)
            result = report_path
        else:
            result = self._wrong_response_message

        return json.dumps(result)

    def get_table_data(self, table: str) -> List[Dict]:
        """
        Get all information from table
        :param table: Table name from database
        :type table: str
        :return: List of tables rows
        :rtype: List[Dict]
        """
        table_data = self._db_client.get_table_data(table)
        return table_data

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
        result = self._db_client.get_url_inheritors(url, **kwargs)
        comments_dict = self._pworker.create_sorted_dict(result, self._keys)

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
            tree = self._db_client.get_comment_inheritors(comment_id)
            tree = self._pworker.cut_paths(tree)
        else:
            tree = self._db_client.get_url_inheritors(url, first_level,
                                                      **kwargs)
        comments_dict = self._pworker.create_sorted_dict(tree, self._keys)
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
        result = self._db_client.get_user_comments(user, **kwargs)
        comments_dict = self._pworker.create_dict(result, self._keys)
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
            result = self._db_client.get_url_inheritors(url, first_level=False,
                                                        **kwargs)
            result_dict = self._pworker.create_dict(result, self._keys)
        else:
            result_dict = {}

        return result_dict
