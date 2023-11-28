# query_execution.py
import pandas as pd
import pymysql
import direct_redis

# Connect to mysql
conn_mysql = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

# Connect to redis
conn_redis = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis
nation_df = conn_redis.get('nation')
supplier_df = conn_redis.get('supplier')
part_df = conn_redis.get('part')

# Filter 'supplier' and 'nation' for Canadian suppliers
canadian_suppliers = supplier_df.merge(nation_df[nation_df['N_NAME'] == 'CANADA'],
                                       left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter 'part' for parts that start with 'forest'
forest_parts = part_df[part_df['P_NAME'].str.startswith('forest')]

# Execute subquery 2 on MySQL to get the part keys
forest_parts_query = 'SELECT PS_PARTKEY FROM partsupp WHERE PS_PARTKEY IN (%s)' % (
    ', '.join(str(part_key) for part_key in forest_parts['P_PARTKEY'].tolist()))
forest_parts_keys = pd.read_sql(forest_parts_query, conn_mysql)

# Execute subquery 3 on MySQL for the threshold calculation
threshold_query = """
SELECT PS_SUPPKEY, PS_PARTKEY FROM partsupp WHERE PS_SUPPKEY IN (%s)
AND PS_PARTKEY IN (%s) AND PS_AVAILQTY > (
    SELECT 0.5 * SUM(L_QUANTITY) FROM lineitem
    WHERE L_PARTKEY = partsupp.PS_PARTKEY
    AND L_SUPPKEY = partsupp.PS_SUPPKEY
    AND L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
)
""" % (', '.join(str(supp_key) for supp_key in canadian_suppliers['S_SUPPKEY'].tolist()),
       ', '.join(str(key) for key in forest_parts_keys['PS_PARTKEY'].tolist()))
qualified_suppliers = pd.read_sql(threshold_query, conn_mysql)

# Combine results to find the Canadian suppliers that meet all the conditions
qualified_canadian_suppliers = canadian_suppliers[canadian_suppliers['S_SUPPKEY'].isin(qualified_suppliers['PS_SUPPKEY'])]

# Select the relevant columns and order the data by supplier name
final_result = qualified_canadian_suppliers[['S_NAME', 'S_ADDRESS']].sort_values(by='S_NAME')

# Write the result to a CSV file
final_result.to_csv('query_output.csv', index=False)

# Close connections
conn_mysql.close()
