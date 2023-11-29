import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Get data from MySQL tables
partsupp_query = "SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST FROM partsupp;"
orders_query = "SELECT O_ORDERKEY, DATE_FORMAT(O_ORDERDATE, '%%Y') as YEAR, O_ORDERDATE FROM orders;"
lineitem_query = """
SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT
FROM lineitem
WHERE L_SHIPDATE BETWEEN O_ORDERDATE AND DATE_ADD(O_ORDERDATE, INTERVAL 1 YEAR);
"""

partsupp_df = pd.read_sql(partsupp_query, mysql_conn)
orders_df = pd.read_sql(orders_query, mysql_conn)
lineitem_df = pd.read_sql(lineitem_query, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis database
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis 'tables'
nation_df = pd.read_json(redis_conn.get('nation'), orient='records')
supplier_df = pd.read_json(redis_conn.get('supplier'), orient='records')
part_df = pd.read_json(redis_conn.get('part'), orient='records')

# Filter part_df for parts containing a specified dim in their names
specified_dim = 'DIM'  # Replace with the actual dim value you are checking
part_df = part_df[part_df['P_NAME'].str.contains(specified_dim)]

# Join and calculate the profit
profit_result = (
    lineitem_df.merge(orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(partsupp_df, how='inner', on=['L_PARTKEY', 'L_SUPPKEY'])
    .merge(supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
)

profit_result['PROFIT'] = (profit_result['L_EXTENDEDPRICE'] * (1 - profit_result['L_DISCOUNT'])) - (profit_result['PS_SUPPLYCOST'] * profit_result['L_QUANTITY'])

# Group by nation and year, then sum the profit
profit_grouped = profit_result.groupby(['N_NAME', 'YEAR'])['PROFIT'].sum().reset_index()

# Sort nations alphabetically, and years in descending order
profit_grouped_sorted = profit_grouped.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False])

# Write results to csv
profit_grouped_sorted.to_csv('query_output.csv', index=False)
