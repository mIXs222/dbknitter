import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
lineitem_query = """
SELECT *
FROM lineitem
WHERE 
    (L_SHIPMODE = 'AIR' OR L_SHIPMODE = 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    AND L_QUANTITY >= 1 AND (
        (L_QUANTITY <= 11 AND L_SIZE BETWEEN 1 AND 5) OR
        (L_QUANTITY BETWEEN 10 AND 20 AND L_SIZE BETWEEN 1 AND 10) OR
        (L_QUANTITY BETWEEN 20 AND 30 AND L_SIZE BETWEEN 1 AND 15)
    )
"""
lineitem_df = pd.read_sql(lineitem_query, mysql_conn)
mysql_conn.close()

# Redis connection and query
redis_conn = DirectRedis(host='redis', port=6379, db=0)
part_df = pd.read_json(redis_conn.get('part'), orient='records')
part_df = part_df[
    (part_df['P_BRAND'].isin(['Brand#12', 'Brand#23', 'Brand#34'])) &
    part_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])
]

# Merging dataframes
merge_df = pd.merge(
    lineitem_df,
    part_df,
    how='inner',
    left_on='L_PARTKEY',
    right_on='P_PARTKEY'
)

# Calculating revenue
merge_df['REVENUE'] = merge_df['L_EXTENDEDPRICE'] * (1 - merge_df['L_DISCOUNT'])

# Writing the results to CSV
merge_df.to_csv('query_output.csv', index=False)
