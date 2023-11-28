import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query MySQL for nation and partsupp tables
with mysql_connection.cursor() as cursor:
    cursor.execute("SELECT * FROM nation WHERE N_NAME = 'GERMANY'")
    nations = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

    cursor.execute("SELECT * FROM partsupp")
    partsupps = pd.DataFrame(cursor.fetchall(), columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])

# Close MySQL connection
mysql_connection.close()

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query Redis for supplier table
suppliers = pd.read_json(redis_connection.get('supplier'), orient='records')

# Merge dataframes based on nation and supplier keys
combined_df = pd.merge(nations, suppliers, left_on='N_NATIONKEY', right_on='S_NATIONKEY')
combined_df = pd.merge(combined_df, partsupps, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Calculate total value
combined_df['TOTAL_VALUE'] = combined_df['PS_SUPPLYCOST'] * combined_df['PS_AVAILQTY']

# Calculate threshold in subquery equivalent
threshold = combined_df['TOTAL_VALUE'].sum() * 0.05  # Example threshold: 5% of overall value

# Filter based on threshold and group by part key
result = combined_df.groupby('PS_PARTKEY').filter(lambda x: x['TOTAL_VALUE'].sum() > threshold)

# Select relevant columns
result = result[['PS_PARTKEY', 'TOTAL_VALUE']]

# Write the final output to CSV
result.to_csv('query_output.csv', index=False)
