import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query to retrieve lineitems with P_PARTKEY, L_EXTENDEDPRICE, and L_QUANTITY
lineitem_query = """
SELECT
    L_ORDERKEY,
    L_PARTKEY,
    L_EXTENDEDPRICE,
    L_QUANTITY
FROM
    lineitem
"""
lineitems_df = pd.read_sql(lineitem_query, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to the Redis database
redis_conn = DirectRedis(host='redis', port=6379, db=0)
part_df = pd.DataFrame(eval(redis_conn.get('part')))

# Filter the parts based on Brand and Container Type
part_filtered_df = part_df[(part_df['P_BRAND'] == 'Brand#23') & (part_df['P_CONTAINER'] == 'MED BAG')]

# Join the two dataframes on P_PARTKEY = L_PARTKEY (for available data in the part dataframe)
merged_df = pd.merge(lineitems_df, part_filtered_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the average quantity per part
avg_qty_per_part = merged_df.groupby('P_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_qty_per_part.rename(columns={'L_QUANTITY': 'AVG_QTY'}, inplace=True)

# Merge the average quantity per part back to the merged_df
merged_df = pd.merge(merged_df, avg_qty_per_part, on='P_PARTKEY')

# Filter line items based on quantity being less than 20% of the average quantity
filtered_lineitems = merged_df[merged_df['L_QUANTITY'] < 0.2 * merged_df['AVG_QTY']]

# Calculate the average yearly extended price (sum of L_EXTENDEDPRICE divided by 7.0)
average_yearly_extended_price = filtered_lineitems['L_EXTENDEDPRICE'].sum() / 7.0

# Write the result to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['average_yearly_extended_price'])
    writer.writerow([average_yearly_extended_price])
