# query.py

import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Establish connection to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Construct query for MySQL
mysql_query = """
SELECT 
    O_ORDERPRIORITY, 
    COUNT(*) as ORDER_COUNT
FROM 
    orders
WHERE 
    O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'
GROUP BY 
    O_ORDERPRIORITY
ORDER BY 
    O_ORDERPRIORITY ASC;
"""

# Read data from MySQL
orders_df = pd.read_sql(mysql_query, mysql_connection)

# Close the MySQL connection
mysql_connection.close()

# Establish connection to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Read data from Redis
lineitem_df = pd.DataFrame(eval(redis_connection.get('lineitem')))

# Close the Redis connection
redis_connection.close()

# Perform the main query logic
filtered_lineitem = lineitem_df[lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']]
joined_data = filtered_lineitem.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')

result = joined_data.groupby('O_ORDERPRIORITY', as_index=False).agg({'O_ORDERKEY':'nunique'}).rename(columns={'O_ORDERKEY':'ORDER_COUNT'}).sort_values('O_ORDERPRIORITY')

# Write the result to file
result[['ORDER_COUNT', 'O_ORDERPRIORITY']].to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)
