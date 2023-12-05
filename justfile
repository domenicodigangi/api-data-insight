default:
  just --list

init:
  poetry config virtualenvs.in-project true
  poetry install
  pip install -e .
start-ch:
    cd /home/ddg/clickhouse && nohup ./clickhouse server &

start-ch-client:
    cd /home/ddg/clickhouse && ./clickhouse client 
