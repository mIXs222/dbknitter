import pymysql
import pymongo
import pandas as pd  
from direct_redis import DirectRedis
import csv

# Establish MySQL connection
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Retrieve data from MySQL
mysql_query = """
SELECT o.O_ORDERDATE, l.L_EXTENDEDPRICE, l.L_DISCOUNT
FROM orders AS o
JOIN lineitem AS l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE l.L_SHIPMODE = 'SMALL PLATED COPPER'
"""
mysql_data = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Process date to only get years and filter for 1995 and 1996
mysql_data['O_ORDERDATE'] = pd.to_datetime(mysql_data['O_ORDERDATE']).dt.year
mysql_data = mysql_data[(mysql_data['O_ORDERDATE'] == 1995) | (mysql_data['O_ORDERDATE'] == 1996)]

# Establish MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Retrieve data from MongoDB
supplier = pd.DataFrame(list(mongodb_db.supplier.find({'S_NATIONKEY': 'IND'})))
supplier['S_SUPPKEY'] = supplier['S_SUPPKEY'].astype(str)

# Retrieve data from Redis using DirectRedis
redis_client = DirectRedis(host='redis', port=6379, db=0)
nation = pd.read_json(redis_client.get('nation'), orient='records')
region = pd.read_json(redis_client.get('region'), orient='records')

# Filter for only ASIA and INDIA
asia = region[region['R_NAME'] == 'ASIA']
india = nation[(nation['N_NAME'] == 'INDIA') & (nation['N_REGIONKEY'].isin(asia['R_REGIONKEY']))]

# Merge the datasets
data_merged = mysql_data.merge(supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Calculate the market share
data_merged['REVENUE'] = data_merged['L_EXTENDEDPRICE'] * (1 - data_merged['L_DISCOUNT'])
market_share = data_merged.groupby('O_ORDERDATE').agg({'REVENUE': 'sum'}).reset_index()
market_share.columns = ['ORDER_YEAR', 'MARKET_SHARE']

# Output to CSV
market_share.to_csv('query_output.csv', index=False)
