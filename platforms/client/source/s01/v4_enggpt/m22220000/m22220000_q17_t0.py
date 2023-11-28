import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
cur = conn.cursor()

# Redis setup
r = DirectRedis(host='redis', port=6379, db=0)

# Get part data from redis
parts_df = pd.read_json(r.get('part'))

# Filter parts with 'Brand#23' brand and 'MED BAG' container type
filtered_parts_df = parts_df[(parts_df['P_BRAND'] == 'Brand#23') & (parts_df['P_CONTAINER'] == 'MED BAG')]

# Execute query on MySQL
lineitem_sql = """
SELECT L_PARTKEY, SUM(L_EXTENDEDPRICE) AS SUM_EXTENDEDPRICE, L_QUANTITY 
FROM lineitem 
GROUP BY L_PARTKEY;
"""
cur.execute(lineitem_sql)
lineitem_result = cur.fetchall()

# Close the MySQL connection
cur.close()
conn.close()

# Convert MySQL result to DataFrame
lineitem_df = pd.DataFrame(lineitem_result, columns=['L_PARTKEY', 'SUM_EXTENDEDPRICE', 'L_QUANTITY'])

# Join the two dataframes on part key
result_df = pd.merge(filtered_parts_df, lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Calculate 20% of the average quantity for the specific part
result_df['AVG_20PCT_QUANTITY'] = result_df.groupby('P_PARTKEY')['L_QUANTITY'].transform(lambda x: 0.2 * x.mean())

# Include only line items where the quantity is less than 20% of the average quantity of the same part
result_df = result_df[result_df['L_QUANTITY'] < result_df['AVG_20PCT_QUANTITY']]

# Sum extended prices and calculate average yearly extended price
result_df['AVERAGE_YEARLY_EXTENDEDPRICE'] = result_df['SUM_EXTENDEDPRICE'] / 7.0

# Select required columns, and avoid duplicating part key columns
final_result_df = result_df[['P_PARTKEY', 'AVERAGE_YEARLY_EXTENDEDPRICE']]

# Save result to file
final_result_df.to_csv('query_output.csv', index=False)
