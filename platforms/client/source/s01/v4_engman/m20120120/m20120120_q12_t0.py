import pymysql
import direct_redis
import pandas as pd

# Connection details for the MySQL database
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Fetching data from MySQL
with mysql_connection.cursor() as cursor:
    query = """
    SELECT
        L_SHIPMODE,
        COUNT(CASE WHEN O_ORDERPRIORITY = '1-URGENT' OR
                         O_ORDERPRIORITY = '2-HIGH' THEN 1 END) AS HIGH_PRIORITY_COUNT,
        COUNT(CASE WHEN O_ORDERPRIORITY != '1-URGENT' AND
                         O_ORDERPRIORITY != '2-HIGH' THEN 1 END) AS LOW_PRIORITY_COUNT
    FROM
        lineitem
    WHERE
        L_SHIPMODE IN ('MAIL', 'SHIP') AND
        L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01' AND
        L_SHIPDATE < L_COMMITDATE AND
        L_RECEIPTDATE > L_COMMITDATE
    GROUP BY
        L_SHIPMODE
    ORDER BY
        L_SHIPMODE ASC;
    """
    cursor.execute(query)
    lineitem_data = cursor.fetchall()

# Transforming MySQL data into DataFrame
lineitem_df = pd.DataFrame(lineitem_data, columns=['L_SHIPMODE', 'HIGH_PRIORITY_COUNT', 'LOW_PRIORITY_COUNT'])

# Connection details for the Redis database
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetching data from Redis and transform to DataFrame
orders_df = pd.read_json(redis_connection.get('orders'))

# Merge the MySQL and Redis data based on criteria
# Assuming O_ORDERPRIORITY represents the priority of lineitems in Redis
merged_df = lineitem_df.merge(orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Grouping by L_SHIPMODE and counting priorities
result_df = merged_df.groupby('L_SHIPMODE').agg({
    'HIGH_PRIORITY_COUNT': 'sum',
    'LOW_PRIORITY_COUNT': 'sum'
}).reset_index()

# Sorting the results as per requirement
result_df.sort_values('L_SHIPMODE', ascending=True, inplace=True)

# Write the query output to a csv file
result_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_connection.close()
redis_connection.close()
