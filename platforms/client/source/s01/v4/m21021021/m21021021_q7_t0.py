import pandas as pd
import pymysql
import pymongo
from datetime import datetime
from direct_redis import DirectRedis

# Function to convert MongoDB cursor to Pandas DataFrame
def mongo_cursor_to_dataframe(cursor):
    return pd.DataFrame(list(cursor))

# Function to format shipdate and calculate volume in lineitems DataFrame
def process_lineitems(lineitems_df):
    lineitems_df['L_SHIPDATE'] = pd.to_datetime(lineitems_df['L_SHIPDATE'])
    lineitems_df['L_YEAR'] = lineitems_df['L_SHIPDATE'].dt.year
    lineitems_df['VOLUME'] = lineitems_df['L_EXTENDEDPRICE'] * (1 - lineitems_df['L_DISCOUNT'])
    return lineitems_df

# MySQL connection and query execution
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
customer_df = pd.read_sql("SELECT * FROM customer", mysql_conn)
orders_df = pd.read_sql("SELECT * FROM orders", mysql_conn)

# MongoDB connection and query execution
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_lineitem_collection = mongo_db['lineitem']
mongo_lineitem_cursor = mongo_lineitem_collection.find()
lineitems_df = mongo_cursor_to_dataframe(mongo_lineitem_cursor)
lineitems_df = process_lineitems(lineitems_df)

# Redis connection and query execution
redis_conn = DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_json(redis_conn.get('nation'), orient='records')
supplier_df = pd.read_json(redis_conn.get('supplier'), orient='records')

# Close database connections
mysql_conn.close()
mongo_client.close()

# SQL-like join and query processing
# Define date range
date_start = datetime(1995, 1, 1)
date_end = datetime(1996, 12, 31)

# Merge DataFrames
merged_df = (
    supplier_df.merge(nation_df.rename(columns={'N_NAME': 'SUPP_NATION', 'N_NATIONKEY': 'S_NATIONKEY'}),
                      on='S_NATIONKEY')
    .merge(lineitems_df, on='S_SUPPKEY')
    .merge(orders_df, on='O_ORDERKEY')
    .merge(customer_df.merge(nation_df.rename(columns={'N_NAME': 'CUST_NATION', 'N_NATIONKEY': 'C_NATIONKEY'}),
                             on='C_NATIONKEY'),
           on='C_CUSTKEY')
)

# Filter rows according to the given conditions
filtered_df = merged_df[
    ((merged_df['SUPP_NATION'] == 'JAPAN') & (merged_df['CUST_NATION'] == 'INDIA') |
     (merged_df['SUPP_NATION'] == 'INDIA') & (merged_df['CUST_NATION'] == 'JAPAN')) &
    (merged_df['L_SHIPDATE'] >= date_start) &
    (merged_df['L_SHIPDATE'] <= date_end)
]

# Grouping and aggregation
result_df = (
    filtered_df.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])
    .agg(REVENUE=('VOLUME', 'sum'))
    .reset_index()
    .sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])
)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
