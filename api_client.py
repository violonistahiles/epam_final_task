import csv
import datetime
import json
import os
from typing import Dict

from sqlalchemy import create_engine

from db_client import DBClient
from tester import create_models, print_tables


class RequestData:
    """Class to store parsed request data"""
    def __init__(self, command, request_data):
        """
        :param command: Requested command
        :type command: str
        :param request_data: Request parameters
        :type request_data: str
        """
        self.command = command
        self.data = request_data


class APIClient:
    """Class to process requests"""
    def __init__(self, engine):
        """
        Initialize class
        :param engine: Object establishing connection to database
        :type engine: sqlalchemy.engine.Engine
        """
        curr_path = os.getcwd()
        commands_file = os.path.join(curr_path, 'commands.json')
        with open(commands_file) as fi:
            self._commands = json.load(fi)

        self._db_client = DBClient(engine)

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

    @staticmethod
    def _save_json(data_dict: Dict, data_dir: str) -> str:
        """
        Save query result to .json file
        :param data_dict: Dictionary with query result
        :type data_dict: Dict
        :param data_dir: Path to report folder
        :type data_dir: str
        :return: Path to saved .json file
        :rtype: str
        """
        data_path = os.path.join(data_dir, 'report.json')
        with open(data_path, 'w') as f:
            json.dump(data_dict, f)
        return data_path

    def _save_csv(self, data: Dict, data_dir: str) -> str:
        """
        Save query result to .csv file
        :param data: Dictionary with values for single line in .csv file
        :type data: Dict
        :param data_dir: Path to report folder
        :type data_dir: str
        :return: Path to saved .csv file
        :rtype: str
        """
        data_path = os.path.join(data_dir, 'report.csv')
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
        command, attrs = self._parse_request(request)
        if command == 'add_comment':
            result = self._db_client.add_comment(**attrs)
        elif command == 'ger_url_first_level_comments':
            result = self._db_client.get_url_comments(**attrs)
        elif command == 'get_comment_tree':
            result = self._db_client.get_comment_tree(**attrs)
        elif command == 'get_user_history':
            result = self._db_client.get_user_history(**attrs)
        elif command == 'get_report':
            data_dict = self._db_client.prepare_data_to_report(**attrs)
            report_dir = os.path.join(os.getcwd(), 'build')
            report_path = self._save_csv(data_dict, report_dir)
            result = report_path
        else:
            result = {}

        return json.dumps(result)


if __name__ == '__main__':
    engine = create_engine('sqlite:///:memory:')
    create_models(engine)
    # request = {'command': 'add_comment',
    #            'parent_id': 1,
    #            'url': 'url_1',
    #            'user': 'Dart',
    #            'comment': 'AXAXA'}
    #
    # request = {'command': 'ger_url_first_level_comments',
    #            'url': 'url_1'}
    #
    # request = {'command': 'get_comment_tree',
    #            'url': 'url_1',
    #            'comment_id': None}
    #
    request = {'command': 'get_user_history',
               'user': 'Luke',
               'do_sort': True}

    # request = {'command': 'get_report',
    #            'url': 'url_1',
    #            'user': 'Dart',
    #            'do_sort': True,
    #            'start': 100,
    #            'end': None}

    print(request)
    api_client = APIClient(engine)
    resp = api_client.process_request(json.dumps(request))
    print(resp)

    print_tables(engine)
