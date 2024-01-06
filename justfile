default:
  just --list

init:
  poetry config virtualenvs.in-project true
  poetry install
  # poetry run pip install -e ./submodules/api-fetcher/ --no-cache-dir
  poetry run pip install -e .
start-ch:
    cd /home/ddg/clickhouse && nohup ./clickhouse server &

build-superset:
    docker build -t superset:local ./superset

start-superset:
    docker run -d --network host  --name superset -p 8080:8080   superset:local

run-flet-app:
    cd /workspaces/public-api-insight/src/web_app && \
    python main.py