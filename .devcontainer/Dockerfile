FROM python:3.10-bullseye
RUN apt-get update
RUN apt-get install redis-server -y
RUN pip install poetry
RUN useradd -m -s /bin/bash -u 1000 ddg


RUN mkdir /home/ddg/clickhouse
WORKDIR /home/ddg/clickhouse
RUN curl https://clickhouse.com/ | sh