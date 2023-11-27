import pymysql
from pymongo import MongoClient
import pandas as pd

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query to select nation from MySQL
my_sql_query = """
SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME IN ('INDIA', 'JAPAN');
"""

# Get nations from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute(my_sql_query)
    nations = list(cursor.fetchall())

# Transform data to DataFrame
nation_df = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME'])

# Get data from MongoDB
supplier_df = pd.DataFrame(list(mongo_db.supplier.find({'S_NATIONKEY': {'$in': nation_df['N_NATIONKEY'].tolist()}})))
customer_df = pd.DataFrame(list(mongo_db.customer.find({'C_NATIONKEY': {'$in': nation_df['N_NATIONKEY'].tolist()}})))

# Install direct_redis with pip if not already installed.
from direct_redis import DirectRedis
# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get orders and lineitem tables as DataFrames from Redis
orders_str = redis_conn.get('orders')
lineitem_str = redis_conn.get('lineitem')
orders_df = pd.read_json(orders_str)
lineitem_df = pd.read_json(lineitem_str)

# Calculate the year from O_ORDERDATE
orders_df['O_YEAR'] = pd.to_datetime(orders_df['O_ORDERDATE']).dt.year
filtered_orders_df = orders_df[(orders_df['O_YEAR'] == 1995) | (orders_df['O_YEAR'] == 1996)]

# Merge with lineitem to compute revenues
merged_df = pd.merge(filtered_orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Compute gross discounted revenue
merged_df['GROSS_DISCOUNTED_REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Merge with supplier and customer, filtering only those from India and Japan
combined_df = pd.merge(
    pd.merge(merged_df, supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY'),
    customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY'
)
filtered_combined_df = combined_df[
    (combined_df['S_NATIONKEY'] != combined_df['C_NATIONKEY'])
    & (combined_df['N_NAME_x'].isin(['INDIA', 'JAPAN']))
    & (combined_df['N_NAME_y'].isin(['INDIA', 'JAPAN']))
]

# Final select and group by
result_df = filtered_combined_df.groupby(['N_NAME_x', 'N_NAME_y', 'O_YEAR'])['GROSS_DISCOUNTED_REVENUE'].sum().reset_index()

# Order the result
result_df = result_df.sort_values(by=['N_NAME_x', 'N_NAME_y', 'O_YEAR'])

# Write to CSV
result_df.to_csv('query_output.csv', index=False)

# Cleanup
mysql_conn.close()
mongo_client.close()
redis_conn.close()
