import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Retrieve data from MySQL
try:
    with mysql_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM nation WHERE N_NAME = 'SAUDI ARABIA'")
        nation_records = cursor.fetchall()

        cursor.execute("SELECT * FROM supplier")
        supplier_records = cursor.fetchall()
finally:
    mysql_conn.close()

# Transform MySQL data into pandas DataFrames
nation_df = pd.DataFrame(nation_records, columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
supplier_df = pd.DataFrame(supplier_records, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Filter nation and supplier dataFrames
nation_df = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']
supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df['N_NATIONKEY'])]

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from Redis
orders_df = pd.read_json(redis_conn.get('orders'), orient='records')
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Filter out orders with O_ORDERSTATUS = 'F'
orders_df = orders_df[orders_df['O_ORDERSTATUS'] == 'F']

# Python processing to simulate the original SQL logic due to cross-database nature of data
L1_df = lineitem_df[lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']].rename(columns=lambda x: 'L1_' + x)
L2_df = lineitem_df.rename(columns=lambda x: 'L2_' + x)

L2_filtered = L2_df[~L2_df['L2_SUPPKEY'].isin(L1_df['L1_SUPPKEY'])]
L1_df = L1_df[L1_df['L1_ORDERKEY'].isin(L2_filtered['L2_ORDERKEY'])]

# Merge dataFrames
merged_df = supplier_df.merge(L1_df, left_on='S_SUPPKEY', right_on='L1_SUPPKEY')
merged_df = merged_df.merge(orders_df, left_on='L1_ORDERKEY', right_on='O_ORDERKEY')

# Group by supplier name and count
final_df = merged_df.groupby('S_NAME').size().reset_index(name='NUMWAIT')

# Sort the results
final_df_sorted = final_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Output the result to a CSV file
final_df_sorted.to_csv('query_output.csv', index=False)
