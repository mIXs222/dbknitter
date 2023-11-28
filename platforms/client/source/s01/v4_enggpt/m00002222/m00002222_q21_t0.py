import pymysql
import pandas as pd
import direct_redis

# Connect to the MySQL database
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Execute the query for suppliers and nations from MySQL
supplier_nation_query = """
SELECT s.S_NAME, s.S_SUPPKEY
FROM supplier s
JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE n.N_NAME = 'SAUDI ARABIA'
"""
suppliers_df = pd.read_sql(supplier_nation_query, mysql_connection)
mysql_connection.close()

# Connect to the Redis database
redis_connection = direct_redis.DirectRedis(host='redis', port=6379)

# Retrieve orders and lineitem from Redis as Pandas DataFrame
orders_df = pd.DataFrame(redis_connection.get('orders'))
lineitem_df = pd.DataFrame(redis_connection.get('lineitem'))

# Filter orders and line items according to the specified conditions
filtered_orders = orders_df[orders_df['O_ORDERSTATUS'] == 'F']
filtered_lineitem = lineitem_df[lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']]

# Merge dataframes to prepare for further processing
merged_df = filtered_lineitem.merge(filtered_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Use conditions specified for EXISTS subqueries in the original query
merged_df = merged_df[merged_df['L_SUPPKEY'].isin(suppliers_df['S_SUPPKEY'])]

# Group by supplier key and count the number of line items for each supplier
result = merged_df.groupby(['L_SUPPKEY']).size().reset_index(name='NUMWAIT')

# Merge with the suppliers dataframe to get supplier names
result = result.merge(suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Select the desired columns and sort results as specified
result = result[['S_NAME', 'NUMWAIT']].sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write the results to a CSV file
result.to_csv('query_output.csv', index=False)
