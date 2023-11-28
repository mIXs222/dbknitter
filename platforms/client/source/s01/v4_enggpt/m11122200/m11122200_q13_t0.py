import pymysql
import pandas as pd
import direct_redis

# Establish connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Querying the MySQL database to retrieve 'orders' data
sql_query = """
SELECT O_CUSTKEY, COUNT(*) AS O_COUNT
FROM orders
WHERE O_COMMENT NOT LIKE '%pending%' AND O_COMMENT NOT LIKE '%deposits%'
GROUP BY O_CUSTKEY
"""
orders_df = pd.read_sql_query(sql_query, mysql_conn)
mysql_conn.close()

# Establish connection to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Getting 'customer' data from Redis
customer_ser = redis_conn.get('customer')
customer_df = pd.read_json(customer_ser, orient='records')

# Merging the dataframes to get the required result
merged_df = customer_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')

# Filling NaN values with 0 in 'O_COUNT' and converting it to int
merged_df['O_COUNT'] = merged_df['O_COUNT'].fillna(0).astype(int)

# Calculating the distribution of customers based on count of their orders (C_COUNT)
distribution_df = merged_df.groupby('O_COUNT').agg(CUSTDIST=('C_CUSTKEY', 'count')).reset_index()

# Ordering the result based on 'CUSTDIST' and 'C_COUNT' descending
distribution_df.sort_values(by=['CUSTDIST', 'O_COUNT'], ascending=[False, False], inplace=True)

# Writing the result to a CSV file
distribution_df.to_csv('query_output.csv', index=False)
