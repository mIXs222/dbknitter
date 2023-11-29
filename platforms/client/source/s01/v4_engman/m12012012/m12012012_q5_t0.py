import pymysql
import pymongo
from direct_redis import DirectRedis
import pandas as pd

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT C_CUSTKEY, C_NATIONKEY FROM customer")
    customer_data = cursor.fetchall()

# Convert the MySQL data into DataFrame
customer_df = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_NATIONKEY'])

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client.tpch

# Get nation and orders data from MongoDB
nation_docs = list(mongodb_db.nation.find({"N_REGIONKEY": "ASIA"}))
orders_docs = list(mongodb_db.orders.find(
    {"O_ORDERDATE": {"$gte": "1990-01-01", "$lt": "1995-01-01"}}
))

# Convert the MongoDB data into DataFrame
nation_df = pd.DataFrame(nation_docs)
orders_df = pd.DataFrame(orders_docs)

# Connect to Redis and get data into DataFrame
redis_client = DirectRedis(host='redis', port=6379, db=0)
region_df = pd.read_json(redis_client.get('region'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filter the relevant nation keys for ASIA region
asia_nations = nation_df[nation_df.N_NAME == 'ASIA']['N_NATIONKEY'].unique()

# Join DataFrames based on nation keys
customer_order_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
asia_orders = customer_order_df[customer_order_df.C_NATIONKEY.isin(asia_nations)]

# Join with lineitem DataFrame and calculate the revenue
result_df = pd.merge(asia_orders, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Group by N_NAME and calculate total revenue per nation
final_df = result_df.groupby('N_NAME', as_index=False)['REVENUE'].sum()
final_df = final_df.sort_values('REVENUE', ascending=False)

# Write the output to CSV
final_df.to_csv('query_output.csv', index=False)
