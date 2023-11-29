import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve parts information from Redis
part_keys = ['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT']
part_data = redis_conn.get('part')

# Parse DataFrame from Redis data
part_df = pd.read_json(part_data)

# Query for parts from Redis according to the given conditions
part_df_conditions = part_df[
    ((part_df['P_BRAND'] == 'Brand#12') &
     (part_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
     (part_df['P_SIZE'] >= 1) & (part_df['P_SIZE'] <= 5)) |
    ((part_df['P_BRAND'] == 'Brand#23') &
     (part_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
     (part_df['P_SIZE'] >= 1) & (part_df['P_SIZE'] <= 10)) |
    ((part_df['P_BRAND'] == 'Brand#34') &
     (part_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
     (part_df['P_SIZE'] >= 1) & (part_df['P_SIZE'] <= 15))
]
part_keys = part_df_conditions['P_PARTKEY'].tolist()

# MySQL part key filter string
part_key_filter = ','.join(map(str, part_keys))

# MySQL query
mysql_query = f"""
SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM lineitem
WHERE L_PARTKEY IN ({part_key_filter})
AND L_SHIPMODE IN ('AIR', 'AIR REG')
AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
"""

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    result = cursor.fetchone()
    
# Save results to a CSV file
output_df = pd.DataFrame([{'REVENUE': result[0]}])
output_df.to_csv('query_output.csv', index=False)

# Close database connections
mysql_conn.close()
