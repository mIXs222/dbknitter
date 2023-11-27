import pymysql
import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis with DirectRedis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch nation from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation")
    nations = {row[0]: row[1] for row in cursor.fetchall()}

# Fetch supplier from MongoDB
supplier_df = pd.DataFrame(list(mongodb['supplier'].find()))

# Fetch customer from MongoDB
customer_df = pd.DataFrame(list(mongodb['customer'].find()))

# Convert Redis data to Pandas DataFrame
orders_df = pd.read_json(redis_conn.get('orders'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Merge dataframes to create a combined dataframe for querying
merged_df = (
    supplier_df.merge(lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
)

# Convert NATIONKEY to nation names
merged_df['SUPP_NATION'] = merged_df['S_NATIONKEY'].map(nations)
merged_df['CUST_NATION'] = merged_df['C_NATIONKEY'].map(nations)

# Filter based on conditions from SQL query
filtered_df = merged_df[
    merged_df['SUPP_NATION'].isin(['JAPAN', 'INDIA']) &
    merged_df['CUST_NATION'].isin(['JAPAN', 'INDIA']) &
    (merged_df['SUPP_NATION'] != merged_df['CUST_NATION']) &
    (merged_df['L_SHIPDATE'].between('1995-01-01', '1996-12-31'))
]

# Add L_YEAR and VOLUME columns
filtered_df['L_YEAR'] = filtered_df['L_SHIPDATE'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').year)
filtered_df['VOLUME'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by the specified columns and calculate REVENUE
result_df = filtered_df.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], as_index=False).agg(REVENUE=('VOLUME', 'sum'))

# Sort the results
result_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], inplace=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
