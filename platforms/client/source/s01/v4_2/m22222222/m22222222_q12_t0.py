import pandas as pd
from redis import Redis
from pandasql import sqldf

def load_data_from_redis(table_name, client):
    data = client.get(table_name)
    df = pd.read_msgpack(data)
    return df

def main():
    client = Redis(db='0', port=6379, host='redis')
    orders = load_data_from_redis('orders', client)
    lineitem = load_data_from_redis('lineitem', client)

    query = """
    SELECT
        L_SHIPMODE,
        SUM(CASE
                WHEN O_ORDERPRIORITY = '1-URGENT'
                OR O_ORDERPRIORITY = '2-HIGH'
                THEN 1
                ELSE 0
        END) AS HIGH_LINE_COUNT,
        SUM(CASE
                WHEN O_ORDERPRIORITY <> '1-URGENT'
                AND O_ORDERPRIORITY <> '2-HIGH'
                THEN 1
                ELSE 0
        END) AS LOW_LINE_COUNT
    FROM
        orders,
        lineitem
    WHERE
        O_ORDERKEY = L_ORDERKEY
        AND L_SHIPMODE IN ('MAIL', 'SHIP')
        AND L_COMMITDATE < L_RECEIPTDATE
        AND L_SHIPDATE < L_COMMITDATE
        AND L_RECEIPTDATE >= '1994-01-01'
        AND L_RECEIPTDATE < '1995-01-01'
    GROUP BY
        L_SHIPMODE
    ORDER BY
        L_SHIPMODE
    """
    result = sqldf(query, locals())
    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
