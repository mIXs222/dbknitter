import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Execute MySQL Query
mysql_query = """
SELECT o.O_ORDERKEY, o.O_CUSTKEY, l.L_EXTENDEDPRICE, l.L_DISCOUNT, c.C_NATIONKEY
FROM orders o
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY
WHERE o.O_ORDERDATE >= '1990-01-01' AND o.O_ORDERDATE <= '1994-12-31';
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    orders_lineitem_data = cursor.fetchall()

# Convert to DataFrame
orders_lineitem_df = pd.DataFrame(orders_lineitem_data, columns=['O_ORDERKEY', 'O_CUSTKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'C_NATIONKEY'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client['tpch']
nation_col = mongo_db['nation']
region_col = mongo_db['region']

# Query MongoDB for Asia
asia_region = region_col.find_one({'R_NAME': 'ASIA'})
asia_nations = list(nation_col.find({'N_REGIONKEY': asia_region['R_REGIONKEY']}))

# Get nation keys from Asia
asia_nation_keys = [nation['N_NATIONKEY'] for nation in asia_nations]

# Filter orders_lineitem_data for Asia
orders_lineitem_asia_df = orders_lineitem_df[orders_lineitem_df['C_NATIONKEY'].isin(asia_nation_keys)]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get DataFrame from Redis
supplier_df = pd.read_json(redis_client.get('supplier'))
customer_df = pd.read_json(redis_client.get('customer'))

# Merge data
customer_orders_lineitem_asia_df = pd.merge(
    orders_lineitem_asia_df, customer_df[['C_CUSTKEY', 'C_NATIONKEY']],
    left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner'
)

# Calculate revenue
customer_orders_lineitem_asia_df['Revenue'] = \
    customer_orders_lineitem_asia_df['L_EXTENDEDPRICE'] * (1 - customer_orders_lineitem_asia_df['L_DISCOUNT'])

# Group by nation and calculate total revenue
nation_revenue_df = customer_orders_lineitem_asia_df.groupby('C_NATIONKEY')['Revenue'].sum().reset_index()

# Merge with nation names
final_df = pd.merge(
    nation_revenue_df, pd.DataFrame(asia_nations)[['N_NATIONKEY', 'N_NAME']],
    left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner'
)

# Sort by total revenue
final_df = final_df.sort_values(by=['Revenue'], ascending=False)

# Renaming the columns for final output
final_df.rename(columns={'N_NAME': 'Nation', 'Revenue': 'Total_Revenue'}, inplace=True)
final_df = final_df[['Nation', 'Total_Revenue']]

# Write the results to a CSV file
final_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_client.close()
