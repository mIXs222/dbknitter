import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Query MySQL for region and lineitem data
query_region = """
SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA'
"""
mysql_cursor.execute(query_region)
regions_asia_keys = [record[0] for record in mysql_cursor.fetchall()]

query_lineitem = """
SELECT L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE 
FROM lineitem
WHERE YEAR(L_SHIPDATE) BETWEEN 1995 AND 1996
"""
lineitems_df = pd.read_sql(query_lineitem, mysql_conn)

# Clean up MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query MongoDB for part and customer data
parts = mongo_db['part']
parts_df = pd.DataFrame(list(parts.find({'P_TYPE': 'SMALL PLATED COPPER'})))

customers = mongo_db['customer']
customers_df = pd.DataFrame(list(customers.find()))

mongo_client.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query Redis for nation, supplier and orders data
nation_df = pd.read_json(redis_conn.get('nation'))
supplier_df = pd.read_json(redis_conn.get('supplier'))
orders_df = pd.read_json(redis_conn.get('orders'))

# Analysis
# Join dataframes based on their relationships
suppliers_india = supplier_df[supplier_df['S_NATIONKEY'] == nation_df[nation_df['N_NAME'] == 'INDIA']['N_NATIONKEY'].iloc[0]]
orders_india = orders_df[orders_df['O_CUSTKEY'].isin(customers_df[customers_df['C_NATIONKEY'].isin(nation_df[nation_df['N_NAME'] == 'INDIA']['N_NATIONKEY'])]['C_CUSTKEY'])]
lineitems_india = lineitems_df[lineitems_df['L_ORDERKEY'].isin(orders_india['O_ORDERKEY'])]
market_df = lineitems_india.merge(parts_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate volume
market_df['VOLUME'] = market_df['L_EXTENDEDPRICE'] * (1 - market_df['L_DISCOUNT'])

# Calculate market share
total_volume = market_df.groupby(market_df['L_SHIPDATE'].dt.year)['VOLUME'].sum()
india_volume = market_df[market_df['O_ORDERKEY'].isin(orders_india['O_ORDERKEY'])].groupby(market_df['L_SHIPDATE'].dt.year)['VOLUME'].sum()
market_share = india_volume / total_volume

# Formatted result
market_share_result = market_share.reset_index()
market_share_result.columns = ['YEAR', 'MARKET_SHARE']
market_share_result.sort_values('YEAR', inplace=True)

# Write output to CSV
market_share_result.to_csv('query_output.csv', index=False)
