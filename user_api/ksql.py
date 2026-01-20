import requests
import json
from time import sleep

KSQLDB_URL = "http://localhost:8088"

CREATE_STREAM_QUERY = """
CREATE STREAM IF NOT EXISTS goods_stream (
    product_id STRING, 
    name STRING,
    brand STRING,
    price STRUCT<amount DOUBLE, currency STRING>
) WITH (
    KAFKA_TOPIC='source.goods-filtered', 
    VALUE_FORMAT='JSON_SR'
);
"""

CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS last_price AS
SELECT
    product_id,
    LATEST_BY_OFFSET(name) AS name,
    LATEST_BY_OFFSET(brand) AS brand,
    CONCAT(
        CAST(LATEST_BY_OFFSET(price->amount) AS STRING), ' ',
        LATEST_BY_OFFSET(price->currency)
    ) AS price_str
FROM goods_stream
GROUP BY product_id
EMIT CHANGES;
"""


def build_pull_prices_query(name="", brand="") -> str:
    if len(name):
        return f"SELECT * FROM last_price WHERE NAME LIKE '%{name}%';"
    if len(brand):
        return f"SELECT * FROM last_price WHERE BRAND LIKE '%{brand}%';"
    return f"SELECT * FROM last_price;"


def run_ksql(ksql: str, is_query=False):
    endpoint = "/query" if is_query else "/ksql"
    url = f"{KSQLDB_URL}{endpoint}"

    payload = {
        "ksql": ksql,
        "streamsProperties": {"auto.offset.reset": "earliest"},
    }

    headers = {"Content-Type": "application/vnd.ksql.v1+json"}

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    response.raise_for_status()
    if is_query:
        results = []
        for line in response.text.splitlines():
            if line.strip():
                results.append(json.loads(line))
        return results
    else:
        return response.json()


def search(name="", brand=""):
    # Create stream and table
    run_ksql(CREATE_STREAM_QUERY)
    run_ksql(CREATE_TABLE_QUERY)

    # pull query

    while True:
        try:
            prices_response = run_ksql(
                build_pull_prices_query(name, brand), is_query=True
            )
            break
        except:
            # wating for ksqldb ready
            sleep(2)
    for row in prices_response:
        if isinstance(row, dict) and "columnNames" in row.keys():
            print(" | ".join(f"{val:<{20}}" for val in row["columnNames"]))

        else:
            print(" | ".join(f"{val:<{20}}" for val in row))


if __name__ == "__main__":
    search(name="JKL")
    search()
