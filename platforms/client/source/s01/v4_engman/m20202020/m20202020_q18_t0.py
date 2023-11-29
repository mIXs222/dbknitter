import pandas as pd
import pymysql
from pandas.io import sql
import redis
import direct_redis
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Query to fetch large volume orders from lineitem table
mysql_query = """
SELECT 
    C_NAME, 
    C_CUSTKEY, 
    L_ORDERKEY, 
    L_QUANTITY,
    L_EXTENDEDPRICE AS O_TOTALPRICE,
    L_SHIPDATE AS O_ORDERDATE
FROM 
    customer 
    INNER JOIN lineitem ON C_CUSTKEY = L_ORDERKEY
WHERE 
    L_QUANTITY > 300
"""

# Execute the query and fetch the results.
lineitem_results = sql.read_sql(mysql_query, mysql_connection)

# Close MySQL connection
mysql_connection.close()

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve orders table as pandas DataFrame
orders_df = redis_connection.get('orders')

# Convert orders_df from JSON string to pandas DataFrame
if orders_df:
    orders_df = pd.read_json(orders_df)

    # Merge lineitem and orders dataframes on order key
    results_df = pd.merge(orders_df, lineitem_results, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Perform the final filtering and ordering
    final_df = results_df[[
        'C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY'
    ]].sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

    # Write result to csv
    final_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
