import pandas as pd
import pymysql
import pymongo
from datetime import datetime
import direct_redis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", port=27017)
mongo_db = mongo_client["tpch"]

# Redis connection
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Function to load data from Redis
def load_redis_data(redis_conn, key):
    data = redis_conn.get(key)
    if data:
        return pd.read_json(data, orient='records')
    else:
        return pd.DataFrame()

# Fetch parts from Redis
df_parts = load_redis_data(redis_conn, 'part')

# Filter parts containing 'dim'
df_parts_dim = df_parts[df_parts['P_NAME'].str.contains('dim')]

# Fetch partsupp from MySQL
mysql_cursor.execute("SELECT * FROM partsupp")
df_partsupp = pd.DataFrame(mysql_cursor.fetchall(), columns=[desc[0] for desc in mysql_cursor.description])

# Fetch lineitem from MySQL
mysql_cursor.execute("SELECT * FROM lineitem")
df_lineitem = pd.DataFrame(mysql_cursor.fetchall(), columns=[desc[0] for desc in mysql_cursor.description])

# Fetch orders from MongoDB
df_orders = pd.DataFrame(list(mongo_db.orders.find()))

# Fetch nation from MongoDB
df_nation = pd.DataFrame(list(mongo_db.nation.find()))

# Fetch supplier from MongoDB
df_supplier = pd.DataFrame(list(mongo_db.supplier.find()))

# Merge and compute profit
df_merged = (
    df_parts_dim
    .merge(df_partsupp, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
    .merge(df_lineitem, how='inner', left_on='P_PARTKEY', right_on='L_PARTKEY') 
    .merge(df_orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(df_supplier, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
    .merge(df_nation, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
)
df_merged['PROFIT'] = (df_merged['L_EXTENDEDPRICE'] * (1 - df_merged['L_DISCOUNT'])) - (df_merged['PS_SUPPLYCOST'] * df_merged['L_QUANTITY'])
df_merged['YEAR'] = pd.to_datetime(df_merged['O_ORDERDATE']).dt.year

# Group by nation and year
df_grouped = df_merged.groupby(['N_NAME', 'YEAR'], as_index=False)['PROFIT'].sum()

# Sort the results
df_grouped_sorted = df_grouped.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False])

# Write to CSV
df_grouped_sorted.to_csv('query_output.csv', index=False)

# Close database connections
mysql_conn.close()
mongo_client.close()
redis_conn.close()
