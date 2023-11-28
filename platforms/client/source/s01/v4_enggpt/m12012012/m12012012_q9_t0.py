import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Query to get parts from MySQL
mysql_query = """
SELECT P_PARTKEY, P_NAME
FROM part
WHERE P_NAME LIKE '%dim%'
"""
mysql_cursor.execute(mysql_query)
part_records = mysql_cursor.fetchall()
parts_df = pd.DataFrame(part_records, columns=['P_PARTKEY', 'P_NAME'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Get data from MongoDB
nation_col = mongo_db['nation']
supplier_col = mongo_db['supplier']
orders_col = mongo_db['orders']

nation_df = pd.DataFrame(list(nation_col.find()))
supplier_df = pd.DataFrame(list(supplier_col.find()))
orders_df = pd.DataFrame(list(orders_col.find()))

# Process orders and nation data to include year information
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
orders_df['YEAR'] = orders_df['O_ORDERDATE'].dt.year
nation_orders = orders_df.merge(nation_df, left_on='O_CUSTKEY', right_on='N_NATIONKEY', how='inner')

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis
partsupp_df = pd.read_json(redis_client.get('partsupp'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Perform analysis
partsupp_df.rename(columns={'PS_PARTKEY': 'P_PARTKEY'}, inplace=True)
lineitem_df.rename(columns={'L_ORDERKEY': 'O_ORDERKEY', 'L_PARTKEY': 'P_PARTKEY', 'L_SUPPKEY': 'S_SUPPKEY', 'L_EXTENDEDPRICE': 'EXTENDEDPRICE'}, inplace=True)
profit_analysis_df = (
    lineitem_df.merge(parts_df, on='P_PARTKEY')
    .merge(supplier_df, left_on='S_SUPPKEY', right_on='S_SUPPKEY')
    .merge(partsupp_df, on=['P_PARTKEY', 'S_SUPPKEY'])
    .merge(nation_orders, on='O_ORDERKEY')
)

profit_analysis_df['PROFIT'] = profit_analysis_df['EXTENDEDPRICE'] - (profit_analysis_df['PS_SUPPLYCOST'] * profit_analysis_df['L_QUANTITY'])

# Group by Nation and Year
profit_summary_df = (
    profit_analysis_df.groupby(['N_NAME', 'YEAR'])['PROFIT']
    .sum()
    .reset_index()
    .sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False])
)

# Write to CSV
profit_summary_df.to_csv('query_output.csv', index=False)

# Cleanup connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
redis_client.close()
