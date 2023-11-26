import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
my_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_query = """
SELECT
    l.L_ORDERKEY, l.L_PARTKEY, l.L_EXTENDEDPRICE, l.L_DISCOUNT, l.L_QUANTITY, l.L_SHIPMODE, l.L_SHIPINSTRUCT
FROM
    lineitem l
WHERE
    l.L_SHIPMODE IN ('AIR', 'AIR REG')
AND l.L_SHIPINSTRUCT = 'DELIVER IN PERSON'
AND (
        (l.L_QUANTITY >= 1 AND l.L_QUANTITY <= 11) OR
        (l.L_QUANTITY >= 10 AND l.L_QUANTITY <= 20) OR
        (l.L_QUANTITY >= 20 AND l.L_QUANTITY <= 30)
    )
"""
mysql_df = pd.read_sql(mysql_query, my_conn)
my_conn.close()

# Redis connection using DirectRedis to read dataframe
redis_client = DirectRedis(host='redis', port=6379, db=0)
part_data = redis_client.get('part')
part_df = pd.read_json(part_data)

# Filter only the needed rows according to the original query conditions
filtered_part_df = part_df[
    ((part_df['P_BRAND'] == 'Brand#12') & (part_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (part_df['P_SIZE'].between(1, 5))) |
    ((part_df['P_BRAND'] == 'Brand#23') & (part_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & (part_df['P_SIZE'].between(1, 10))) |
    ((part_df['P_BRAND'] == 'Brand#34') & (part_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (part_df['P_SIZE'].between(1, 15)))
]

# Join both DataFrames
result_df = pd.merge(mysql_df, filtered_part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate REVENUE
result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Sum up the REVENUE
total_revenue = result_df['REVENUE'].sum()

# Output to a csv file
final_result = pd.DataFrame({'REVENUE': [total_revenue]})
final_result.to_csv('query_output.csv', index=False)
