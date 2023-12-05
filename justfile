default:
  just --list

start-ch:
    cd /home/ddg/clickhouse && nohup ./clickhouse server &

start-ch-client:
    cd /home/ddg/clickhouse && ./clickhouse client 
