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
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load data from MySQL
part_query = "SELECT * FROM part WHERE P_TYPE = 'SMALL PLATED COPPER'"
customer_query = "SELECT * FROM customer"

with mysql_conn.cursor() as cursor:
    cursor.execute(part_query)
    part_df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])

    cursor.execute(customer_query)
    customer_df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])

# Load data from MongoDB
nation_collection = mongo_db['nation']
region_collection = mongo_db['region']

nation_df = pd.DataFrame(list(nation_collection.find({})))
region_df = pd.DataFrame(list(region_collection.find({"R_NAME": "ASIA"})))

# Load data from Redis
order_df = pd.read_json(redis_client.get('orders'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))
supplier_df = pd.read_json(redis_client.get('supplier'))

# Merge all dataframes
merged_df = pd.merge(part_df, lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')
merged_df = pd.merge(merged_df, supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = pd.merge(merged_df, nation_df.rename({'N_NATIONKEY': 'S_NATIONKEY'}, axis=1), on='S_NATIONKEY')
merged_df = pd.merge(merged_df, order_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = pd.merge(merged_df, customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = pd.merge(merged_df, nation_df.rename({'N_NATIONKEY': 'C_NATIONKEY'}, axis=1), on='C_NATIONKEY')
merged_df = pd.merge(merged_df, region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filter rows based on date and add a year column
merged_df['O_ORDERDATE'] = pd.to_datetime(merged_df['O_ORDERDATE'])
merged_df['O_YEAR'] = merged_df['O_ORDERDATE'].dt.year
filtered_df = merged_df[(merged_df['O_ORDERDATE'] >= '1995-01-01') & (merged_df['O_ORDERDATE'] <= '1996-12-31')]

# Calculate volume and nation
filtered_df['VOLUME'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
filtered_df['NATION'] = filtered_df['N_NAME']

# Group by year and calculate market share
result = filtered_df.groupby('O_YEAR').apply(
    lambda x: (x[x['NATION'] == 'INDIA']['VOLUME'].sum()) / x['VOLUME'].sum()
).reset_index(name='MKT_SHARE')

# Write to CSV
result.to_csv('query_output.csv', index=False)

# Clean up connections
mysql_conn.close()
mongo_client.close()
redis_client.close()
