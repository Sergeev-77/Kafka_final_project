#!/bin/bash
set -e

NN_DIR=/usr/local/hadoop/hdfs/namenode

mkdir -p "$NN_DIR"
chmod -R 777 /usr/local/hadoop/hdfs/namenode


if [ ! -f "$NN_DIR/current/VERSION" ]; then
    echo "Formatting HDFS NameNode..."
hdfs namenode -format -force -nonInteractive
fi


hdfs --daemon start namenode

until hdfs dfs -ls / >/dev/null 2>&1; do
    echo "Waiting for NameNode..."
sleep 2
done

hdfs dfs -mkdir -p /data/kafka || true
hdfs dfs -chown -R appuser:supergroup /data || true
hdfs dfs -chmod -R 775 /data || true

hdfs --daemon stop namenode

exec "$@"
