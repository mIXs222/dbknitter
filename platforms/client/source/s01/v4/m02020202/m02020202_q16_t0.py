import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Establish a connection to the MySQL database
connection_mysql = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Create a cursor object using the cursor() method
cursor_mysql = connection_mysql.cursor()

# Prepare SQL query for MySQL database
query_mysql = """
    SELECT
        part.P_PARTKEY,
        part.P_BRAND,
        part.P_TYPE,
        part.P_SIZE,
        partsupp.PS_SUPPKEY
    FROM
        part JOIN partsupp ON part.P_PARTKEY = partsupp.PS_PARTKEY
    WHERE
        part.P_BRAND <> 'Brand#45'
        AND part.P_TYPE NOT LIKE 'MEDIUM POLISHED%'
        AND part.P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
"""

# Execute the SQL command
cursor_mysql.execute(query_mysql)

# Fetch all the rows in a list of lists.
data_mysql = cursor_mysql.fetchall()

# Create a DataFrame from MySQL data
df_mysql = pd.DataFrame(data_mysql, columns=['P_PARTKEY', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'PS_SUPPKEY'])

# Establish a connection to the Redis database
connection_redis = DirectRedis(host="redis", port=6379, db=0)

# Prepare keys and fetch data from Redis database for the relevant table
supplier_data = connection_redis.get('supplier')
df_redis = pd.read_json(supplier_data)

# Filter Redis DataFrame based on condition
df_redis_filtered = df_redis[~df_redis['S_COMMENT'].str.contains('Customer%Complaints%')]

# Convert the DataFrame to set for quick lookup
excluded_suppkeys = set(df_redis_filtered['S_SUPPKEY'].unique())

# Filter MySQL DataFrame based on excluded suppkeys
df_mysql_filtered = df_mysql[~df_mysql['PS_SUPPKEY'].isin(excluded_suppkeys)]

# Group by and count distinct suppliers
aggregated_data = df_mysql_filtered.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['PS_SUPPKEY'].nunique().reset_index(name='SUPPLIER_CNT')

# Sorting the results
aggregated_data.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Save results to a CSV file
aggregated_data.to_csv('query_output.csv', index=False)

# Close the MySQL database connection
cursor_mysql.close()
connection_mysql.close()
