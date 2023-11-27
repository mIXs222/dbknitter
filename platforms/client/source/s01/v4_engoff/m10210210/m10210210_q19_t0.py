import pymysql
import pandas as pd
import direct_redis

# MySQL Connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
cursor = mysql_conn.cursor()

# Get the lineitem table data from MySQL
query = """
SELECT L_EXTENDEDPRICE, L_DISCOUNT, L_QUANTITY, L_SHIPMODE
FROM lineitem
WHERE L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
"""
cursor.execute(query)
results = cursor.fetchall()
mysql_conn.close()

# Column names for the lineitem table
lineitem_cols = ['L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_QUANTITY', 'L_SHIPMODE']
lineitem_df = pd.DataFrame(list(results), columns=lineitem_cols)

# Redis Connection
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the part table data from Redis
part_df = pd.read_json(redis_conn.get('part'), orient='index')

# Merge the dataframes based on P_PARTKEY and L_PARTKEY
merged_df = part_df.merge(lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY', suffixes=('_part', '_lineitem'))

# Filtering based on the conditions for three different types of parts
conditions = [
    ((merged_df['P_BRAND'] == 'Brand#12') &
     (merged_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
     (merged_df['L_QUANTITY'] >= 1) & (merged_df['L_QUANTITY'] <= 11) &
     (merged_df['P_SIZE'] >= 1) & (merged_df['P_SIZE'] <= 5)),

    ((merged_df['P_BRAND'] == 'Brand#23') &
     (merged_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
     (merged_df['L_QUANTITY'] >= 10) & (merged_df['L_QUANTITY'] <= 20) &
     (merged_df['P_SIZE'] >= 1) & (merged_df['P_SIZE'] <= 10)),

    ((merged_df['P_BRAND'] == 'Brand#34') &
     (merged_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
     (merged_df['L_QUANTITY'] >= 20) & (merged_df['L_QUANTITY'] <= 30) &
     (merged_df['P_SIZE'] >= 1) & (merged_df['P_SIZE'] <= 15))
]

# Calculate the gross discounted revenue for the filtered data
filtered_df = merged_df[conditions[0] | conditions[1] | conditions[2]]
filtered_df['DISCOUNTED_REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by L_ORDERKEY and sum the revenue
grouped_df = filtered_df.groupby('L_ORDERKEY')['DISCOUNTED_REVENUE'].sum().reset_index()

# Write to CSV file
grouped_df.to_csv('query_output.csv', index=False)
