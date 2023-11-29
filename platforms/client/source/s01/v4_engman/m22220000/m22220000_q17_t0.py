import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connection information for MySQL and Redis
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

redis_conn_info = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_info)
# Get lineitem DataFrame
lineitem_query = 'SELECT * FROM lineitem;'
lineitem_df = pd.read_sql(lineitem_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(**redis_conn_info)
# Assume part Dataframe is stored with key 'part'
part_df = pd.read_msgpack(redis_conn.get('part'))

# Filter out the part with BRAND#23 and MED BAG
filtered_parts = part_df[(part_df['P_BRAND'] == 'BRAND#23') & (part_df['P_CONTAINER'] == 'MED BAG')]

# Join the filtered parts with lineitem on part key
joined_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(filtered_parts['P_PARTKEY'])]

# Calculate the average lineitem quantity
average_quantity = joined_df['L_QUANTITY'].mean()

# Find lineitems with quantity less than 20% of the average
small_quantity_orders = joined_df[joined_df['L_QUANTITY'] < 0.2 * average_quantity]

# Calculate the yearly gross loss
small_quantity_orders['YEARLY_LOSS'] = small_quantity_orders['L_EXTENDEDPRICE'] * (1 - small_quantity_orders['L_DISCOUNT'])
average_yearly_loss = small_quantity_orders.groupby(small_quantity_orders['L_SHIPDATE'].dt.year)['YEARLY_LOSS'].mean()

# Output to query_output.csv
average_yearly_loss.to_csv('query_output.csv')
