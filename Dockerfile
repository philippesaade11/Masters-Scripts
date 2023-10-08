FROM python:3.8

LABEL maintainer="philippe.saade@tum.de"

COPY ./requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt

COPY ./neo4j_add_edges.py /app/neo4j_add_edges.py

CMD [ "python", "-u", "neo4j_add_edges.py" ]