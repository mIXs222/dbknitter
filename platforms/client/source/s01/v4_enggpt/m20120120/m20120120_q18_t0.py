import pymysql
import pymongo
import pandas as pd
import direct_redis
from sqlalchemy import create_engine

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn.cursor() as cursor:
    # Subquery to get Order Keys with total quantity over 300
    cursor.execute("""
        SELECT L_ORDERKEY, SUM(L_QUANTITY) as total_quantity
        FROM lineitem
        GROUP BY L_ORDERKEY
        HAVING SUM(L_QUANTITY) > 300;
    """)
    qualifying_orders = cursor.fetchall()
    qualifying_order_keys = [order[0] for order in qualifying_orders]

# Convert list to tuple for SQL use
qualifying_order_keys_tuple = tuple(qualifying_order_keys)

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
mongo_cursor = mongo_db['customer'].find({}, {"_id": 0})

# Convert MongoDB data to pandas DataFrame
customer_df = pd.DataFrame(list(mongo_cursor))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
orders_df_str = redis_client.get('orders')
orders_df = pd.read_json(orders_df_str)

# Filter the orders DataFrame with qualifying order keys
filtered_orders_df = orders_df[orders_df['O_ORDERKEY'].isin(qualifying_order_keys)]

# Merge customer and orders DataFrame on 'C_CUSTKEY'
merged_df = pd.merge(customer_df, filtered_orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Load lineitem data from MySQL
df_lineitem = pd.read_sql("SELECT * FROM lineitem WHERE L_ORDERKEY IN {}".format(qualifying_order_keys_tuple), con=mysql_conn)

# Calculate the sum of quantity group by L_ORDERKEY in lineitem DataFrame
lineitem_grouped = df_lineitem.groupby('L_ORDERKEY')['L_QUANTITY'].sum().reset_index()

# Merge results with the final DataFrame
final_df = pd.merge(merged_df, lineitem_grouped, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Additional merge to get order details (since lineitem data doesn't have price)
orders_details = filtered_orders_df[['O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']]
final_df = pd.merge(final_df, orders_details, on='O_ORDERKEY')

# Select relevant fields and set alias
output_df = final_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Sort the results
output_df = output_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write to CSV
output_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_client.close()
