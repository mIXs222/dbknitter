import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']
lineitem_collection = mongo_db['lineitem']

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Pandas DataFrame creation from Redis
supplier_df = pd.read_json(redis_conn.get('supplier').decode('utf-8'), orient='records')
partsupp_df = pd.read_json(redis_conn.get('partsupp').decode('utf-8'), orient='records')

# Query MySQL tables
mysql_cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation")
nation_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME'])

mysql_cursor.execute("SELECT P_PARTKEY, P_TYPE FROM part")
part_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_TYPE'])

# Query MongoDB collections
lineitem_cur = lineitem_collection.find({}, {'_id': 0})
lineitem_df = pd.DataFrame(list(lineitem_cur))

orders_cur = orders_collection.find({}, {'_id': 0})
orders_df = pd.DataFrame(list(orders_cur))

# Close all the connections
mysql_conn.close()
mongo_client.close()
redis_conn.close()

# Start processing
# Filter parts with a specific dim in their names (assuming 'dim' is already defined)
# If the 'dim' is not predefined, you will need to define it before this line.
parts_with_specific_dim_df = part_df[part_df['P_TYPE'].str.contains('dim')]

# Join operations across databases
lineitem_with_parts_df = lineitem_df.merge(parts_with_specific_dim_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
full_join_df = lineitem_with_parts_df.merge(partsupp_df, on=['L_PARTKEY', 'L_SUPPKEY'], how='inner')
full_join_df = full_join_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
full_join_df = full_join_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
full_join_df = full_join_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Calculate profit
full_join_df['profit'] = (full_join_df['L_EXTENDEDPRICE'] * (1 - full_join_df['L_DISCOUNT'])) - (full_join_df['PS_SUPPLYCOST'] * full_join_df['L_QUANTITY'])

# Extract year from O_ORDERDATE
full_join_df['year'] = full_join_df['O_ORDERDATE'].apply(lambda x: x.year)

# Group by nation and year and calculate sum of profit
profit_by_nation_year = full_join_df.groupby(['N_NAME', 'year'])['profit'].sum().reset_index().rename(columns={'N_NAME': 'nation'})

# Sort results
sorted_profit = profit_by_nation_year.sort_values(by=['nation', 'year'], ascending=[True, False])

# Write to CSV
sorted_profit.to_csv('query_output.csv', index=False)
