import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Define query for MySQL
mysql_query = """
SELECT lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT) as discounted_revenue
FROM lineitem
WHERE
    lineitem.L_QUANTITY >= 1 AND lineitem.L_QUANTITY <= 30 AND
    lineitem.L_SHIPMODE IN ('AIR', 'AIR REG') AND
    lineitem.L_LINESTATUS = 'F'
"""

# Execute MySQL query
lineitem_df = pd.read_sql(mysql_query, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis and get 'part' table
redis_conn = DirectRedis(host='redis', port=6379, db=0)
part_df = pd.read_json(redis_conn.get('part'))

# Process part dataframe to filter and select rows as per the user query
# Since Redis cannot process complex SQL-like queries, we will do it in pandas.

# Define conditions for brands and containers selection
brand_containers = {
    12: ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'],
    23: ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'],
    34: ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'],
}

# Process part DataFrame with conditions
part_conditions_df = pd.concat([
    part_df[(part_df['P_BRAND'] == f'Brand#{k:02d}') & (part_df['P_CONTAINER'].isin(v)) & (part_df['P_SIZE'] >= 1) & (part_df['P_SIZE'] <= 5)] for k, v in brand_containers.items()
], ignore_index=True)

# Final merge of the dataframes on L_PARTKEY == P_PARTKEY
result_df = pd.merge(lineitem_df, part_conditions_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Write the combined result to CSV file
result_df.to_csv('query_output.csv', index=False)
