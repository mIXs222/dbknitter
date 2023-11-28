import pymysql
import pymongo
import pandas as pd
import direct_redis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load data from MySQL
mysql_cursor.execute("SELECT * FROM part WHERE P_TYPE = 'SMALL PLATED COPPER';")
parts = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

# Load data from MongoDB
suppliers = pd.DataFrame(list(mongodb.supplier.find()))
orders = pd.DataFrame(list(mongodb.orders.find({'O_ORDERDATE': {'$gte': datetime(1995, 1, 1), '$lte': datetime(1996, 12, 31)}})))

# Load data from Redis
region_data = pd.read_msgpack(redis_conn.get('region'))
lineitem_data = pd.read_msgpack(redis_conn.get('lineitem'))

# Filtering data based on the 'ASIA' region
asia_region_keys = region_data[region_data['R_NAME'] == 'ASIA']['R_REGIONKEY'].tolist()
asia_nations = pd.DataFrame(mongodb.nation.find({'N_REGIONKEY': {'$in': asia_region_keys}}))

# Filtering 'INDIA' nation data
india_nation_key = asia_nations[asia_nations['N_NAME'] == 'INDIA']['N_NATIONKEY'].iloc[0]

# Merging data
merged_data = pd.merge(lineitem_data, 
                       parts, 
                       left_on='L_PARTKEY', 
                       right_on='P_PARTKEY')

merged_data = pd.merge(merged_data,
                       suppliers,
                       left_on='L_SUPPKEY',
                       right_on='S_SUPPKEY')

merged_data = pd.merge(merged_data,
                       orders,
                       left_on='L_ORDERKEY',
                       right_on='O_ORDERKEY')

merged_data['YEAR'] = merged_data['O_ORDERDATE'].dt.year
merged_data = merged_data[(merged_data['YEAR'] >= 1995) & (merged_data['YEAR'] <= 1996)]
merged_data['VOLUME'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT'])

# Filtering 'INDIA' orders
india_orders = merged_data[merged_data['S_NATIONKEY'] == india_nation_key]

# Calculating market share
market_share_by_year = india_orders.groupby('YEAR')['VOLUME'].sum() / merged_data.groupby('YEAR')['VOLUME'].sum()
market_share_by_year = market_share_by_year.reset_index()
market_share_by_year.columns = ['YEAR', 'MARKET_SHARE']

# Writing to a CSV file
market_share_by_year.sort_values('YEAR').to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
mongo_client.close()
redis_conn.close()
