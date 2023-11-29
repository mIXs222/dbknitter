# query.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL server
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    db='tpch'
)

# Connect to MongoDB server
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Connect to Redis server
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get nation data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME = 'INDIA' OR N_NAME = 'JAPAN'")
    nations = cursor.fetchall()

# Create a nation map
nation_map = {row[0]: row[1] for row in nations}

# Get lineitem and orders data from MongoDB
orders_coll = mongodb_db['orders']
lineitem_coll = mongodb_db['lineitem']

lineitem_df = pd.DataFrame(list(lineitem_coll.find(
    {'L_SHIPDATE': {'$gte': '1995-01-01', '$lt': '1997-01-01'}},
    {'_id': 0, 'L_ORDERKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1, 'L_SUPPKEY': 1}
)))

orders_df = pd.DataFrame(list(orders_coll.find(
    {},
    {'_id': 0, 'O_ORDERKEY': 1, 'O_CUSTKEY': 1}
)))

# Compute the result
df_merged = pd.merge(lineitem_df, orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Get supplier and customer data from Redis
supplier_df = pd.read_json(redis_conn.get('supplier'))
customer_df = pd.read_json(redis_conn.get('customer'))
customer_df = customer_df[customer_df['C_NATIONKEY'].isin(nation_map.keys())]
supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_map.keys())]

# Merge supplier and customer with the previouly merged DF
df_merged = df_merged.merge(supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
df_merged = df_merged.merge(customer_df, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Process year and compute revenue
df_merged['L_YEAR'] = pd.to_datetime(df_merged['L_SHIPDATE']).dt.year
df_merged['REVENUE'] = df_merged['L_EXTENDEDPRICE'] * (1 - df_merged['L_DISCOUNT'])

# Filter nation conditions
df_result = df_merged[
    (df_merged['S_NATIONKEY'] != df_merged['C_NATIONKEY']) &
    ((df_merged['S_NATIONKEY'].isin(nation_map.keys())) & (df_merged['C_NATIONKEY'].isin(nation_map.keys())))
]

# Select necessary fields and rename columns
df_result = df_result[['C_NATIONKEY', 'L_YEAR', 'REVENUE', 'S_NATIONKEY']]
df_result = df_result.rename(columns={'C_NATIONKEY': 'CUST_NATION', 'S_NATIONKEY': 'SUPP_NATION'})
df_result['CUST_NATION'] = df_result['CUST_NATION'].apply(lambda x: nation_map[x])
df_result['SUPP_NATION'] = df_result['SUPP_NATION'].apply(lambda x: nation_map[x])

# Sort by conditions
df_result.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], inplace=True)

# Write output to CSV
df_result.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongodb_client.close()
redis_conn.close()
