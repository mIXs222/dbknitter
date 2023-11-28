# Python code to execute the query (query.py)
import pymysql
import pandas as pd
import direct_redis

# Establish a connection to the MySQL database
connection_mysql = pymysql.connect(host='mysql',
                                  user='root',
                                  password='my-secret-pw',
                                  db='tpch')

# Query to get supplier and nation data from MySQL
query_mysql = """
SELECT s.S_SUPPKEY, s.S_NAME, n.N_NAME, s.S_ACCTBAL
FROM supplier s
JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE n.N_NAME = 'GERMANY';
"""

# Execute the query and create a pandas DataFrame
with connection_mysql.cursor() as cursor:
    cursor.execute(query_mysql)
    supplier_nation = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'N_NAME', 'S_ACCTBAL'])

connection_mysql.close()

# Connect to the Redis database
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read the 'partsupp' table from Redis
partsupp = pd.read_json(r.get('partsupp'), orient='split')

# Combine the data from MySQL and Redis
combined_data = pd.merge(supplier_nation, partsupp, how='inner', on='S_SUPPKEY')

# Calculate the total value
combined_data['TOTAL_VALUE'] = combined_data['PS_SUPPLYCOST'] * combined_data['PS_AVAILQTY']

# Filter to get only parts with a value higher than the threshold calculated as a subquery
threshold = combined_data['TOTAL_VALUE'].sum() * 0.05  # Assuming 5% is the threshold
filtered_data = combined_data.groupby('PS_PARTKEY').filter(lambda x: x['TOTAL_VALUE'].sum() > threshold)

# Group by part key and order by total value in descending order
result = filtered_data.groupby('PS_PARTKEY')['TOTAL_VALUE'].sum().reset_index()
result_sorted = result.sort_values(by='TOTAL_VALUE', ascending=False)

# Write the results to a CSV file
result_sorted.to_csv('query_output.csv', index=False)
