# execute_query.py

import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Setup MySQL connection
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor
)

# Setup DirectRedis connection
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Function to execute a query in MySQL and return a dataframe
def fetch_mysql(query):
  with mysql_connection.cursor() as cursor:
      cursor.execute(query)
      columns = [col[0] for col in cursor.description]
      data = cursor.fetchall()
      return pd.DataFrame(data, columns=columns)

# Function to fetch redis table and return a dataframe
def fetch_redis(table_name):
  data = eval(redis_connection.get(table_name).decode('utf-8'))
  return pd.DataFrame(data)

# Fetch required tables from MySQL
nation_df = fetch_mysql("SELECT * FROM nation")
region_df = fetch_mysql("SELECT * FROM region")
supplier_df = fetch_mysql("SELECT * FROM supplier")

# Fetch required tables from Redis
lineitem_df = fetch_redis('lineitem')

# Filter region and nation to include only those in Asia and India respectively
region_df = region_df[region_df.R_NAME == 'ASIA']
nation_df = nation_df[nation_df.N_NAME == 'INDIA']

# Get suppliers from India
indian_suppliers = supplier_df[supplier_df.S_NATIONKEY.isin(nation_df.N_NATIONKEY)]

# Merge tables to get the Asian suppliers information 
asian_lineitems = lineitem_df.merge(indian_suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Filtering for the product type 'SMALL PLATED COPPER'
asian_lineitems = asian_lineitems[asian_lineitems.P_TYPE == 'SMALL PLATED COPPER']

# Calculate the revenue
asian_lineitems['revenue'] = asian_lineitems.L_EXTENDEDPRICE * (1 - asian_lineitems.L_DISCOUNT)

# only consider the years 1995 and 1996
asian_lineitems = asian_lineitems[asian_lineitems.L_SHIPDATE.str.contains('1995|1996')]

# Calculate market share for years
market_share = asian_lineitems.groupby(asian_lineitems.L_SHIPDATE.str[:4])['revenue'].sum().reset_index()
market_share.columns = ['order_year', 'market_share']

# Output to CSV
market_share.to_csv('query_output.csv', index=False)
