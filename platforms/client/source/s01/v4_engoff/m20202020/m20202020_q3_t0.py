import pymysql
import pandas as pd
import direct_redis

# Connect to mysql
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw',
                             database='tpch', charset='utf8mb4', cursorclass=pymysql.cursors.Cursor)

try:
    # Query to select necessary data from MySQL
    mysql_query = """
    SELECT
        l_orderkey,
        l_extendedprice * (1 - l_discount) as revenue
    FROM
        lineitem
    WHERE
        l_shipdate > '1995-03-15'
    ORDER BY
        revenue DESC
    """

    with mysql_conn.cursor() as cursor:
        cursor.execute(mysql_query)
        lineitem_df = pd.DataFrame(cursor.fetchall(), columns=['l_orderkey', 'revenue'])
finally:
    mysql_conn.close()

# Connect to redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve orders from Redis
orders_data = redis_conn.get('orders')
orders_df = pd.read_json(orders_data)

# Merge datasets from different databases and filter out the market segment
combined_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='l_orderkey')
filtered_df = combined_df[combined_df['O_MKTSEGMENT'] == 'BUILDING']

# Sort the results
sorted_df = filtered_df.sort_values(by='revenue', ascending=False)

# Selecting the required columns and writing to a CSV
result_df = sorted_df[['O_ORDERKEY', 'O_ORDERPRIORITY', 'revenue']]
result_df.to_csv('query_output.csv', index=False)
