import pandas as pd
import pymysql
from pymongo import MongoClient
import direct_redis
import datetime as dt

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
redis_get = redis_conn.get  # Function to retrieve data using keys

# Get data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM orders WHERE O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'")
    orders = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

    cursor.execute("SELECT * FROM lineitem")
    lineitem = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

# Get data from MongoDB
part = pd.DataFrame(list(mongodb.part.find({"P_TYPE": "SMALL PLATED COPPER"})))
nation = pd.DataFrame(list(mongodb.nation.find()))
region = pd.DataFrame(list(mongodb.region.find({"R_NAME": "ASIA"})))

# Get data from Redis
supplier = pd.read_json(redis_get('supplier').decode('utf-8'))
customer = pd.read_json(redis_get('customer').decode('utf-8'))

# Merge the datasets
df = (orders
      .merge(lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
      .merge(customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
      .merge(nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
      .merge(region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
      .merge(supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY', suffixes=('_CUS', '_SUP'))
      .merge(part, left_on='L_PARTKEY', right_on='P_PARTKEY')
     )

# Calculate Volume
df['Volume'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])

# Filter for ASIA region and INDIA nation
df_asia_india = df[(df['R_NAME'] == 'ASIA') & (df['N_NAME'] == 'INDIA')]

# Sum volume per year for INDIA and calculate total volume per year
df_asia_india['Year'] = df_asia_india['O_ORDERDATE'].apply(lambda x: x.year)
india_volume_per_year = df_asia_india.groupby('Year')['Volume'].sum().reset_index(name='INDIA_Volume')
total_volume_per_year = df.groupby('Year')['Volume'].sum().reset_index(name='Total_Volume')

# Calculate Market Share
market_share = india_volume_per_year.merge(total_volume_per_year, on='Year')
market_share['Market_Share'] = market_share['INDIA_Volume'] / market_share['Total_Volume']

# Sort based on year and output to a CSV file
market_share.sort_values('Year').to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
