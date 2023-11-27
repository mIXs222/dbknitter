# multi_db_query.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connection details
mysql_conn_details = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}
mongodb_conn_details = {
    'host': 'mongodb',
    'port': 27017,
    'db': 'tpch'
}
redis_conn_details = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_details)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_conn = pymongo.MongoClient(**mongodb_conn_details)
mongodb_db = mongodb_conn[mongodb_conn_details['db']]

# Connect to Redis
redis_conn = DirectRedis(**redis_conn_details)

# Designate keyword for the Product Type
specified_dim = 'SPECIFIED_DIM'

# Execute the MySQL queries
# Get supplier nation
mysql_cursor.execute('SELECT S_SUPPKEY, N_NATIONKEY, N_NAME FROM supplier JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY;')
suppliers_nations = pd.DataFrame(mysql_cursor.fetchall(), columns=['S_SUPPKEY', 'N_NATIONKEY', 'N_NAME'])

# Get orders
mysql_cursor.execute('SELECT O_ORDERKEY, YEAR(O_ORDERDATE) AS O_YEAR, L_EXTENDEDPRICE, L_DISCOUNT, L_QUANTITY, L_SUPPKEY FROM orders JOIN lineitem ON orders.O_ORDERKEY = lineitem.L_ORDERKEY;')
orders_lineitems = pd.DataFrame(mysql_cursor.fetchall(), columns=['O_ORDERKEY', 'O_YEAR', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_QUANTITY', 'L_SUPPKEY'])

# Close MySQL connection
mysql_conn.close()

# Fetch partsupp from MongoDB
partsupp_df = pd.DataFrame(list(mongodb_db.partsupp.find({}, {'_id': 0})))

# Fetch part from Redis and convert to DataFrame
part_raw_data = redis_conn.get('part')
if part_raw_data:
    part_df = pd.read_json(part_raw_data.decode('utf-8'))

# Merge dataframes to calculate profit
merged_df = part_df.merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY', how='inner')
merged_df = merged_df.merge(orders_lineitems, left_on='PS_SUPPKEY', right_on='L_SUPPKEY', how='inner')
merged_df = merged_df.merge(suppliers_nations, on='S_SUPPKEY', how='inner')

# Filter the parts containing the specified dimension in their names
filtered_df = merged_df[merged_df['P_NAME'].str.contains(specified_dim)]

# Calculate profit
filtered_df['PROFIT'] = (filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])) - (filtered_df['PS_SUPPLYCOST'] * filtered_df['L_QUANTITY'])

# Group by nation and year, then sum profit
profit_by_nation_year = filtered_df.groupby(['N_NAME', 'O_YEAR']).agg({'PROFIT': 'sum'}).reset_index()

# Sort the results according to the instructions
profit_by_nation_year.sort_values(by=['N_NAME', 'O_YEAR'], ascending=[True, False], inplace=True)

# Write results to CSV
profit_by_nation_year.to_csv('query_output.csv', index=False)
