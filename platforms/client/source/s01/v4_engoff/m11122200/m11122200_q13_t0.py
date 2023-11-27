# File: multi_db_query.py

import pandas as pd
import pymysql
from direct_redis import DirectRedis

def query_mysql():
    conn_info = {
        'host': 'mysql',
        'user': 'root',
        'password': 'my-secret-pw',
        'db': 'tpch'
    }
    try:
        conn = pymysql.connect(**conn_info)
        cursor = conn.cursor()
        query = """
        SELECT O_CUSTKEY, COUNT(*) AS order_count
        FROM orders
        WHERE O_ORDERSTATUS NOT IN ('pending', 'deposits')
        GROUP BY O_CUSTKEY
        """
        cursor.execute(query)
        mysql_df = pd.DataFrame(cursor.fetchall(), columns=['C_CUSTKEY', 'order_count'])
    finally:
        cursor.close()
        conn.close()
    return mysql_df

def query_redis():
    redis_info = {
        'host': 'redis',
        'port': 6379,
        'db': 0
    }
    redis_client = DirectRedis(**redis_info)
    customer_data = redis_client.get('customer')
    redis_df = pd.read_json(customer_data)
    return redis_df

def main():
    mysql_data = query_mysql()
    redis_data = query_redis()
    combined_data = pd.merge(redis_data, mysql_data, on='C_CUSTKEY', how='left')
    combined_data['order_count'] = combined_data['order_count'].fillna(0).astype(int)
    distribution = combined_data.groupby('order_count').size().reset_index(name='customer_count')
    distribution.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
