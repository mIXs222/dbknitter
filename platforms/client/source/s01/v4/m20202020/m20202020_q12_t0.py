# query_code.py
import pymysql
import pandas as pd
import direct_redis

# Establish connection to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Query for MySQL to select lineitem data
mysql_query = """
SELECT 
    L_ORDERKEY,
    L_SHIPMODE
FROM 
    lineitem
WHERE 
    L_SHIPMODE IN ('MAIL', 'SHIP') 
    AND L_COMMITDATE < L_RECEIPTDATE
    AND L_SHIPDATE < L_COMMITDATE
    AND L_RECEIPTDATE >= '1994-01-01'
    AND L_RECEIPTDATE < '1995-01-01'
"""

# Execute the MySQL query and store the result in a DataFrame
lineitem_df = pd.read_sql(mysql_query, mysql_connection)
mysql_connection.close()

# Establish connection to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve the 'orders' table from Redis
orders_df = pd.DataFrame(eval(redis_connection.get('orders')))

# Merge data from MySQL and Redis
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Apply the conditions and calculate the HIGH_LINE_COUNT and LOW_LINE_COUNT
merged_df['HIGH_LINE_COUNT'] = ((merged_df['O_ORDERPRIORITY'] == '1-URGENT') |
                                (merged_df['O_ORDERPRIORITY'] == '2-HIGH')).astype(int)
merged_df['LOW_LINE_COUNT'] = ((merged_df['O_ORDERPRIORITY'] != '1-URGENT') &
                               (merged_df['O_ORDERPRIORITY'] != '2-HIGH')).astype(int)

# Group by L_SHIPMODE and perform the aggregation required
result_df = merged_df.groupby('L_SHIPMODE').agg(
    HIGH_LINE_COUNT=pd.NamedAgg(column='HIGH_LINE_COUNT', aggfunc='sum'),
    LOW_LINE_COUNT=pd.NamedAgg(column='LOW_LINE_COUNT', aggfunc='sum')
).reset_index()

# Sort the results as per the query
result_df = result_df.sort_values(by='L_SHIPMODE')

# Write the resulting DataFrame to a CSV file
result_df.to_csv('query_output.csv', index=False)
