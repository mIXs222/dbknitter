import pymysql
import pymongo
import pandas as pd
import csv
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

# Retrieve parts from Redis where part names contain 'dim'
part_df = pd.read_json(redis_client.get('part').decode('utf-8'))
dim_part_df = part_df[part_df['P_NAME'].str.contains('dim')]

# Retrieve supplier data from MySQL
supplier_df = pd.read_sql("SELECT * FROM supplier", mysql_conn)

# Retrieve partsupp data from MySQL
partsupp_df = pd.read_sql("SELECT * FROM partsupp", mysql_conn)

# Retrieve lineitem data from MongoDB
lineitem_df = pd.DataFrame(list(mongo_db['lineitem'].find()))

# Retrieve orders data from MongoDB
orders_df = pd.DataFrame(list(mongo_db['orders'].find()))

# Retrieve nation data from Redis
nation_df = pd.read_json(redis_client.get('nation').decode('utf-8'))

# Close the MySQL connection
mysql_conn.close()

# Combine the parts and partsupp dataframes on partkey
part_partsupp_df = dim_part_df.merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Combine the suppliers data
part_partsupp_supplier_df = part_partsupp_df.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Combine the nation data
part_partsupp_supplier_nation_df = part_partsupp_supplier_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Combine the lineitem data
lineitem_order_supplier_nation_df = lineitem_df.merge(part_partsupp_supplier_nation_df, left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['P_PARTKEY', 'PS_SUPPKEY'])

# Combine the orders data and convert order date to year
lineitem_order_supplier_nation_df = lineitem_order_supplier_nation_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
lineitem_order_supplier_nation_df['O_ORDERYEAR'] = pd.to_datetime(lineitem_order_supplier_nation_df['O_ORDERDATE']).dt.year

# Calculate profit
lineitem_order_supplier_nation_df['PROFIT'] = (lineitem_order_supplier_nation_df['L_EXTENDEDPRICE'] * (1 - lineitem_order_supplier_nation_df['L_DISCOUNT'])) - (lineitem_order_supplier_nation_df['PS_SUPPLYCOST'] * lineitem_order_supplier_nation_df['L_QUANTITY'])

# Group by nation and year
profit_nation_year_df = lineitem_order_supplier_nation_df.groupby(['N_NAME', 'O_ORDERYEAR']).agg(TOTAL_PROFIT=pd.NamedAgg(column='PROFIT', aggfunc='sum')).reset_index()

# Sort the results
profit_nation_year_df = profit_nation_year_df.sort_values(by=['N_NAME', 'O_ORDERYEAR'], ascending=[True, False])

# Write to CSV file
profit_nation_year_df.to_csv('query_output.csv', index=False)
