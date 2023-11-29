import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query to get parts information from MySQL
part_query = """
SELECT P_PARTKEY, P_BRAND, P_TYPE
FROM part
WHERE P_BRAND = 'BRAND#23' AND P_TYPE LIKE 'MED BAG%'
"""
part_df = pd.read_sql(part_query, mysql_connection)
mysql_connection.close()

# Redis connection
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Getting lineitem data from Redis
lineitem_df = pd.read_msgpack(redis_connection.get('lineitem'))

# Filter lineitem data for relevant parts using pandas
relevant_parts_keys = part_df['P_PARTKEY'].tolist()
filtered_lineitem_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(relevant_parts_keys)]

# Compute the average quantity and further process
if not filtered_lineitem_df.empty:
    avg_quantity = filtered_lineitem_df['L_QUANTITY'].mean()
    small_qty_threshold = avg_quantity * 0.2
    small_qty_df = filtered_lineitem_df[filtered_lineitem_df['L_QUANTITY'] < small_qty_threshold]

    # Determine the yearly loss
    small_qty_df['YEAR'] = pd.to_datetime(small_qty_df['L_SHIPDATE']).dt.year
    small_qty_df['LOSS'] = small_qty_df['L_EXTENDEDPRICE'] * small_qty_df['L_DISCOUNT']
    yearly_loss = small_qty_df.groupby('YEAR')['LOSS'].sum().mean()

    # Save output to csv
    yearly_loss_df = pd.DataFrame({'Average_Yearly_Loss': [yearly_loss]})
    yearly_loss_df.to_csv('query_output.csv', index=False)
