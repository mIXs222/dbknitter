# query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

def fetch_mysql_data():
    conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
    try:
        query = """
        SELECT
            c.C_NAME,
            c.C_CUSTKEY,
            l.L_ORDERKEY,
            SUM(l.L_QUANTITY) AS total_quantity
        FROM
            customer AS c
        JOIN
            lineitem AS l ON c.C_CUSTKEY = l.L_SUPPKEY
        GROUP BY
            c.C_CUSTKEY, l.L_ORDERKEY
        HAVING
            SUM(l.L_QUANTITY) > 300
        """
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()

def fetch_redis_data():
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    df_orders = pd.read_json(redis_client.get('orders'), orient='records')
    return df_orders

def main():
    mysql_data = fetch_mysql_data()
    redis_data = fetch_redis_data()

    # Combine datasets from different databases using a left join on 'C_CUSTKEY' and 'L_ORDERKEY'
    result = pd.merge(mysql_data, redis_data, how='left', left_on=['C_CUSTKEY', 'L_ORDERKEY'], right_on=['O_CUSTKEY', 'O_ORDERKEY'])

    # Select the required columns
    result = result[['C_NAME', 'C_CUSTKEY', 'L_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'total_quantity']]

    # Write the result to the file
    result.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
