# query.py
import pymysql
import pandas as pd
import direct_redis

# Connection information for MySQL
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connection information for Redis
redis_conn_info = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Connect to MySQL and execute the query to fetch orders data
with pymysql.connect(**mysql_conn_info) as connection:
    with connection.cursor() as cursor:
        mysql_query = """
            SELECT
                O_ORDERKEY,
                O_ORDERPRIORITY,
                O_ORDERDATE
            FROM
                orders
            WHERE
                O_ORDERDATE >= '1993-07-01'
                AND O_ORDERDATE < '1993-10-01'
        """
        cursor.execute(mysql_query)
        orders_df = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_ORDERPRIORITY', 'O_ORDERDATE'])

# Connect to Redis and retrieve lineitem data
direct_redis_client = direct_redis.DirectRedis(**redis_conn_info)
lineitem_df = direct_redis_client.get('lineitem')
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])
filtered_lineitem_df = lineitem_df[lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']]

# Filter orders that have a corresponding lineitem
filtered_orders_df = orders_df[orders_df['O_ORDERKEY'].isin(filtered_lineitem_df['L_ORDERKEY'])]

# Group by O_ORDERPRIORITY and count
result_df = filtered_orders_df.groupby('O_ORDERPRIORITY', as_index=False).size()
result_df.columns = ['O_ORDERPRIORITY', 'ORDER_COUNT']

# Sort the results
result_df.sort_values(by='O_ORDERPRIORITY', inplace=True)

# Output to a CSV file
result_df.to_csv('query_output.csv', index=False)
