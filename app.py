from flask import Flask, render_template, request
from sqlalchemy import create_engine

from db_backend.api_client import APIClient

app = Flask(__name__)


class App:
    """Class for handling http requests"""
    def __init__(self, engine):
        """
        :param engine: Object establishing connection to database
        :type engine: sqlalchemy.engine.Engine
        """
        self._tables = ['users', 'urls', 'comments']
        self.api_client = APIClient(engine)

    def run(self):
        """Process requests from web application"""
        if request.method == 'GET':
            tables = {table: self.api_client.get_table_data(table)
                      for table in self._tables}
            return render_template('bootstrap_data.html',
                                   response='',
                                   **tables)

        elif request.method == 'POST':
            request_data = request.form['textfield']
            data = self.api_client.process_request(request_data)
            tables = {table: self.api_client.get_table_data(table)
                      for table in self._tables}
            return render_template('bootstrap_data.html',
                                   response=data,
                                   **tables)
        else:
            return render_template('bootstrap_data.html')


if __name__ == '__main__':
    engine = create_engine('sqlite:///database/main.db')
    # app.config["SQLALCHEMY_DATABASE_URI"] = 'sqlite:///main.db'

    application = App(engine)
    app.add_url_rule("/", view_func=application.run, methods=['GET', 'POST'])
    app.run(debug=True, host='0.0.0.0')
