FROM python:3

RUN mkdir -p /home/app

COPY ./db_backend /home/app/db_backend
COPY ./templates /home/app/templates
COPY ./commands /home/app/commands
COPY ./app.py /home/app
COPY ./requirements.txt /home/app

ENV FLASK_APP=app.py

WORKDIR /home/app

RUN pip install -r requirements.txt

ENTRYPOINT ["flask"]

CMD ["run", "--host", "0.0.0.0" ]
