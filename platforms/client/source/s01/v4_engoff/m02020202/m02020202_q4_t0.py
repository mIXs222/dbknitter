import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

try:
    # Query orders table in MySQL
    with mysql_conn.cursor() as cursor:
        query = """
        SELECT O_ORDERPRIORITY, COUNT(*) AS order_count
        FROM orders
        WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'
        AND O_ORDERKEY IN (
            SELECT L_ORDERKEY
            FROM lineitem
            WHERE L_RECEIPTDATE > L_COMMITDATE
        )
        GROUP BY O_ORDERPRIORITY
        ORDER BY O_ORDERPRIORITY;
        """
        cursor.execute(query)
        orders_data = cursor.fetchall()
finally:
    mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis
lineitem_df = pd.DataFrame(redis_conn.get('lineitem'))
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
late_lineitems = lineitem_df[lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']]

# Convert MySQL data to DataFrame
orders_df = pd.DataFrame(orders_data, columns=['O_ORDERPRIORITY', 'order_count'])
orders_df = orders_df[orders_df['O_ORDERPRIORITY'].isin(late_lineitems['L_ORDERKEY'])]

# Write to csv
orders_df.to_csv('query_output.csv', index=False)
