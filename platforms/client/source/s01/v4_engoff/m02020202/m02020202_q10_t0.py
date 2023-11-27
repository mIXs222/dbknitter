import pymysql
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connection details
mysql_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor()

# Retrieving orders within the specified date range from MySQL
orders_query = """
SELECT O_CUSTKEY, SUM(O_TOTALPRICE*(1-L_DISCOUNT)) AS revenue_lost
FROM orders
INNER JOIN lineitem ON O_ORDERKEY = L_ORDERKEY
WHERE L_RETURNFLAG = 'R' AND O_ORDERDATE BETWEEN '1993-10-01' AND '1994-01-01'
GROUP BY O_CUSTKEY;
"""
mysql_cursor.execute(orders_query)
orders_result = mysql_cursor.fetchall()

# Create a dataframe from orders query result
orders_df = pd.DataFrame(orders_result, columns=['C_CUSTKEY', 'revenue_lost'])

# Sort and prepare the data for merging
orders_df['C_CUSTKEY'] = orders_df['C_CUSTKEY'].astype(int)
orders_df.sort_values(by=['revenue_lost', 'C_CUSTKEY'], ascending=[False, True], inplace=True)

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Get customer data from Redis and convert to DataFrame
customer_data = redis.get('customer')
customer_df = pd.read_pickle(customer_data, compression=None)

# Merge orders_df with customer_df
merged_df = pd.merge(orders_df, customer_df, on='C_CUSTKEY')

# Sort the merged dataframe according to the specified conditions, dropping duplicated customer keys while keeping the first entry
final_df = merged_df.drop_duplicates(subset=['C_CUSTKEY']).sort_values(by=['revenue_lost', 'C_CUSTKEY', 'C_ACCTBAL'], ascending=[False, True, False])

# Save to CSV
final_df.to_csv('query_output.csv', index=False)
