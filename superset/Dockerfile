FROM apache/superset
USER root
RUN pip install clickhouse-connect
USER superset
ENV SUPERSET_SECRET_KEY=nTU/xWxju3/QrO8N2lMpWTc8AAvBRPUYYYs4K4v/RcwAsjb6Ddb7Lldv
RUN superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password admin

RUN superset db upgrade
RUN superset load_examples
RUN superset init
