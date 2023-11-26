import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Fetch supplier and partsupp from MySQL
mysql_cursor.execute("SELECT * FROM supplier")
supplier = pd.DataFrame(mysql_cursor.fetchall(), columns=[
                        'S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

mysql_cursor.execute("SELECT * FROM partsupp")
partsupp = pd.DataFrame(mysql_cursor.fetchall(), columns=[
                        'PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_tpch = mongo_client['tpch']

# Fetch orders and lineitem from MongoDB
orders = pd.DataFrame(list(mongo_tpch['orders'].find()))
lineitem = pd.DataFrame(list(mongo_tpch['lineitem'].find()))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch nation and part from Redis
nation = pd.read_msgpack(redis_client.get('nation'))
part = pd.read_msgpack(redis_client.get('part'))

# Filter parts with names like '%dim%'
part = part[part['P_NAME'].str.contains('dim')]

# Perform the JOIN operations
joined_data = (
    part.merge(supplier, left_on='P_PARTKEY', right_on='S_NATIONKEY')
        .merge(lineitem, left_on='P_PARTKEY', right_on='L_PARTKEY')
        .merge(partsupp, on=['PS_PARTKEY', 'PS_SUPPKEY'])
        .merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
        .merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
)

# Compute the calculation for amount
joined_data['AMOUNT'] = (joined_data['L_EXTENDEDPRICE'] * (1 - joined_data['L_DISCOUNT']) - (
    joined_data['PS_SUPPLYCOST'] * joined_data['L_QUANTITY']))

# Convert O_ORDERDATE to year and add as O_YEAR
joined_data['O_YEAR'] = joined_data['O_ORDERDATE'].apply(
    lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').year)

# Group by nation and year
result = joined_data.groupby(['NATION', 'O_YEAR']).agg({'AMOUNT': 'sum'}).reset_index()

# Sort the results
result.sort_values(by=['NATION', 'O_YEAR'], ascending=[True, False], inplace=True)

# Write to CSV
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
