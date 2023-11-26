import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL database
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Query the MySQL database
mysql_query = """
SELECT
    L_ORDERKEY,
    L_PARTKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_QUANTITY,
    L_SHIPMODE,
    L_SHIPINSTRUCT
FROM
    lineitem
WHERE
    (L_QUANTITY >= 1 AND L_QUANTITY <= 11
    AND L_SHIPMODE IN ('AIR', 'AIR REG')
    AND L_SHIPINSTRUCT = 'DELIVER IN PERSON')
    OR
    (L_QUANTITY >= 10 AND L_QUANTITY <= 20
    AND L_SHIPMODE IN ('AIR', 'AIR REG')
    AND L_SHIPINSTRUCT = 'DELIVER IN PERSON')
    OR
    (L_QUANTITY >= 20 AND L_QUANTITY <= 30
    AND L_SHIPMODE IN ('AIR', 'AIR REG')
    AND L_SHIPINSTRUCT = 'DELIVER IN PERSON')
"""

with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    lineitem_data = cursor.fetchall()
    lineitem_df = pd.DataFrame(lineitem_data, columns=[
        'L_ORDERKEY', 'L_PARTKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_QUANTITY',
        'L_SHIPMODE', 'L_SHIPINSTRUCT'
    ])

# Close MySQL connection
mysql_connection.close()

# Connect to Redis database
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query the Redis database and convert it into a DataFrame
part_df = pd.DataFrame(eval(r.get('part')), columns=[
    'P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE',
    'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'
])

# Convert part keys to integer for merge operation
part_df['P_PARTKEY'] = part_df['P_PARTKEY'].astype(int)

# Inner join on 'L_PARTKEY' and filter based on conditions
merged_df = pd.merge(
    lineitem_df, part_df, how='inner',
    left_on='L_PARTKEY', right_on='P_PARTKEY'
)

# Apply all filter conditions
conditions = (
    ((merged_df['P_BRAND'] == 'Brand#12') &
    (merged_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
    (merged_df['P_SIZE'].between(1, 5))) |
    
    ((merged_df['P_BRAND'] == 'Brand#23') &
    (merged_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
    (merged_df['P_SIZE'].between(1, 10))) |
    
    ((merged_df['P_BRAND'] == 'Brand#34') &
    (merged_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
    (merged_df['P_SIZE'].between(1, 15)))
)

filtered_df = merged_df[conditions]

# Calculate the revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by order and calculate total revenue
result_df = filtered_df.groupby('L_ORDERKEY', as_index=False)['REVENUE'].sum()

# Write the result to a CSV
result_df.to_csv('query_output.csv', index=False)
