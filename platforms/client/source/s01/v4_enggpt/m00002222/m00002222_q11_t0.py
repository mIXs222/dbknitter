import pymysql
import pandas as pd
import csv
from direct_redis import DirectRedis

# Connect to mysql
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cur = mysql_conn.cursor()

# Execute the mysql part of the query (for supplier and nation)
mysql_cur.execute(
    """
    SELECT 
        S_SUPPKEY, 
        N_NAME 
    FROM 
        supplier,
        nation 
    WHERE 
        S_NATIONKEY = N_NATIONKEY 
        AND N_NAME = 'GERMANY'
    """
)
suppliers_in_germany = mysql_cur.fetchall()

# Create a DataFrame from the suppliers_in_germany data
suppliers_df = pd.DataFrame(list(suppliers_in_germany), columns=['S_SUPPKEY', 'N_NAME'])

# Connect to redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve partsupp data from redis and convert to DataFrame
partsupp_data = redis_client.get('partsupp')
partsupp_df = pd.read_csv(partsupp_data.decode())

# Merge the DataFrames on S_SUPPKEY
merged_df = pd.merge(suppliers_df, partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Calculate the total value for each part
merged_df['TOTAL_VALUE'] = merged_df['PS_SUPPLYCOST'] * merged_df['PS_AVAILQTY']

# Calculate the threshold value in a subquery-like fashion
threshold_value = merged_df['TOTAL_VALUE'].sum() * 0.05  # Assuming threshold is 5% of overall value

# Group by part key and filter groups by the threshold
grouped = merged_df.groupby('PS_PARTKEY').filter(lambda x: x['TOTAL_VALUE'].sum() > threshold_value)

# Sort and write to CSV
output_df = grouped.sort_values(by='TOTAL_VALUE', ascending=False)
output_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_cur.close()
mysql_conn.close()
