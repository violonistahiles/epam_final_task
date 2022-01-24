from flask import Flask, render_template, request
from sqlalchemy import create_engine

from api_client import APIClient
from db_table import CommentsDB, URLsDB, UserDB
from tester import print_tables

app = Flask(__name__)


class App:
    def __init__(self, engine):
        self.api_client = APIClient(engine)

    def run(self):

        if request.method == 'GET':
            users = self.api_client.get_table_info(UserDB)
            urls = self.api_client.get_table_info(URLsDB)
            comments = self.api_client.get_table_info(CommentsDB)

            return render_template('bootstrap_data.html',
                                   users=users,
                                   urls=urls,
                                   comments=comments,
                                   response='')

        elif request.method == 'POST':
            request_data = request.form['textfield']
            # print(request_data, type(request_data))
            data = self.api_client.process_request(request_data)
            # print(data)
            users = self.api_client.get_table_info(UserDB)
            urls = self.api_client.get_table_info(URLsDB)
            comments = self.api_client.get_table_info(CommentsDB)

            return render_template('bootstrap_data.html',
                                   users=users,
                                   urls=urls,
                                   comments=comments,
                                   response=data)

        else:
            return render_template('bootstrap_data.html')


if __name__ == '__main__':
    engine = create_engine('sqlite:///main.db')
    app.config["SQLALCHEMY_DATABASE_URI"] = 'sqlite:///main.db'
    print_tables(engine)

    application = App(engine)
    application.api_client.get_table_info(UserDB)

    app.add_url_rule("/", view_func=application.run, methods=['GET', 'POST'])
    app.run()
