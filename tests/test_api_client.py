import json

from db_backend.api_client import APIClient, RequestData


class FakeDBClient:
    def __init__(self, engine):
        self.engine = engine
        self.keys = []

    def add_comment(self, *args, **kwargs):
        pass

    def get_url_inheritors(self, *args, **kwargs):
        return {'a': 'b'}

    def get_user_comments(self, *args, **kwargs):
        return {'a': 'b'}

    def get_comment_inheritors(self, *args, **kwargs):
        return {'a': 'b'}

    def get_table_data(self, *args, **kwargs):
        return {'a': 'b'}


class FakePathWorker:

    def create_sorted_dict(self, *args, **kwargs):
        return args[0]

    def cut_paths(self, *args, **kwargs):
        return args[0]

    def create_dict(self, *args, **kwargs):
        return args[0]


def test_parse_request():

    test_command = '{"command": "get_comment_tree", "url": "url_1"}'
    api_client = APIClient('fake_engine')
    correct_result = RequestData('get_comment_tree', {"url": "url_1",
                                                      "comment_id": None})

    test_result = api_client._parse_request(test_command)

    assert test_result.command == correct_result.command
    assert test_result.attrs == correct_result.attrs


def test_process_request():

    test_command = '{"command": "get_comment_tree", "url": "url_1"}'
    api_client = APIClient('fake_engine')
    api_client._db_client = FakeDBClient('Fake_engine')
    api_client._pworker = FakePathWorker()

    correct_result = json.dumps({'a': 'b'})

    test_result = api_client.process_request(test_command)

    assert test_result == correct_result
