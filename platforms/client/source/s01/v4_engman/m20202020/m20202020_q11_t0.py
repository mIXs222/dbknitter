# important_stock_identification.py
import pymysql
import pandas as pd
import direct_redis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Query to get suppliers in Germany
german_suppliers_query = """
SELECT S_SUPPKEY FROM supplier
WHERE S_NATIONKEY = (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY')
"""

# Execute the query
mysql_cursor.execute(german_suppliers_query)
german_suppliers = mysql_cursor.fetchall()

# Create a DataFrame from fetched data
german_suppliers_keys = [supplier[0] for supplier in german_suppliers]
df_german_suppliers = pd.DataFrame(german_suppliers_keys, columns=['PS_SUPPKEY'])

# Redis connection (using direct_redis for pandas support)
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get partsupp table as pandas DataFrame from Redis
partsupp_df = redis_conn.get('partsupp')
partsupp_df = pd.read_json(partsupp_df.decode())

# Filter partsupp table for German suppliers
filtered_partsupp_df = partsupp_df[partsupp_df['PS_SUPPKEY'].isin(german_suppliers_keys)]

# Calculate the total value of the stock of each part from German suppliers and filter them
filtered_partsupp_df['PS_TOTVALUE'] = filtered_partsupp_df['PS_AVAILQTY'] * filtered_partsupp_df['PS_SUPPLYCOST']
total_value = filtered_partsupp_df['PS_TOTVALUE'].sum()
important_stocks_df = filtered_partsupp_df[filtered_partsupp_df['PS_TOTVALUE'] > 0.0001 * total_value]

# Select relevant columns
important_stocks_df = important_stocks_df[['PS_PARTKEY', 'PS_TOTVALUE']]

# Sort in descending order of value
important_stocks_df.sort_values(by='PS_TOTVALUE', ascending=False, inplace=True)

# Write result to file
important_stocks_df.to_csv('query_output.csv', index=False)

# Cleanup
mysql_cursor.close()
mysql_conn.close()
