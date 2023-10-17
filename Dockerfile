FROM python:3.8

LABEL maintainer="philippe.saade@tum.de"

COPY ./requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt

COPY ./weaviate_add.py /app/weaviate_add.py

CMD [ "python", "-u", "weaviate_add.py" ]