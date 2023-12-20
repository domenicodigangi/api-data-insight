default:
  just --list

init:
  poetry config virtualenvs.in-project true
  poetry install
  # poetry run pip install -e ./submodules/api-fetcher/ --no-cache-dir
  poetry run pip install -e .
start-ch:
    cd /home/ddg/clickhouse && nohup ./clickhouse server &

start-ch-client:
    cd /home/ddg/clickhouse && ./clickhouse client 

launch-superset:
    docker run -d --network host  --name superset -p 8080:8080 -e SUPERSET_SECRET_KEY=nTU/xWxju3/QrO8N2lMpWTc8AAvBRPUYYYs4K4v/RcwAsjb6Ddb7Lldv  superset:local
init-superset:
    docker exec -it superset superset fab create-admin \
                  --username admin \
                  --firstname Superset \
                  --lastname Admin \
                  --email admin@superset.com \
                  --password admin
    docker exec -it superset superset db upgrade
    docker exec -it superset superset load_examples
    docker exec -it superset superset init

start-superset:
    just launch-superset
    just init-superset