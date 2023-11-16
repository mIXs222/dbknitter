import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Fetch MySQL data (part)
mysql_cursor.execute(
    "SELECT * FROM part WHERE "
    "(P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5) "
    "OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10) "
    "OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)"
)
part_rows = mysql_cursor.fetchall()
df_part = pd.DataFrame(part_rows, columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to Redis and fetch data (lineitem)
redis_conn = DirectRedis(host='redis', port=6379, db=0)
df_lineitem = pd.read_msgpack(redis_conn.get('lineitem'))

# Merge data on P_PARTKEY = L_PARTKEY and apply filters
merged_df = pd.merge(df_lineitem, df_part, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Apply filters to the merged dataframe
filtered_df = merged_df[((merged_df['P_BRAND'] == 'Brand#12') &
                         (merged_df['L_QUANTITY'] >= 1) & (merged_df['L_QUANTITY'] <= 11) &
                         (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
                         (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) |
                        ((merged_df['P_BRAND'] == 'Brand#23') &
                         (merged_df['L_QUANTITY'] >= 10) & (merged_df['L_QUANTITY'] <= 20) &
                         (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
                         (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) |
                        ((merged_df['P_BRAND'] == 'Brand#34') &
                         (merged_df['L_QUANTITY'] >= 20) & (merged_df['L_QUANTITY'] <= 30) &
                         (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
                         (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'))]

# Calculate revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by nothing, just to use the summation function
grouped_df = filtered_df.groupby(lambda x: True).agg({'REVENUE': 'sum'})

# Write the result to CSV
grouped_df.to_csv('query_output.csv', index=False)
