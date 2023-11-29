import pymysql
import pandas as pd
import direct_redis

# Establish a connection to the MySQL server
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Prepare MySQL query
mysql_query = """
SELECT 
    P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE
FROM
    part
WHERE
    P_BRAND <> 'Brand#45' AND 
    P_TYPE NOT LIKE '%MEDIUM POLISHED%' AND 
    P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
"""

# Execute the query and fetch the data into a pandas DataFrame
parts_dataframe = pd.read_sql(mysql_query, con=mysql_conn)

# Close the MySQL connection
mysql_conn.close()

# Establish a connection to the Redis server
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve partsupp data from Redis
partsupp_series = redis_conn.get('partsupp')
partsupp_dataframe = pd.read_json(partsupp_series)

# Merge the parts dataframe with the partsupp dataframe
result = pd.merge(parts_dataframe, partsupp_dataframe, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Count the number of suppliers that can supply the parts
result_count = result.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['PS_SUPPKEY'].count().reset_index()
result_count.rename(columns={'PS_SUPPKEY': 'SUPPLIER_COUNT'}, inplace=True)

# Sort the result in descending order of count and ascending order of brand, type, and size
result_sorted = result_count.sort_values(by=['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write the result to a CSV file
result_sorted.to_csv('query_output.csv', index=False)
