import pymysql
import pymongo
from datetime import datetime
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Define the timeframe of interest
start_date = datetime(1995, 1, 1)
end_date = datetime(1996, 12, 31)

# Read nation from MySQL
nation_df = pd.read_sql('SELECT * FROM nation', mysql_conn)

# Filter nation for 'JAPAN' and 'INDIA'
nation_df = nation_df[(nation_df['N_NAME'] == 'JAPAN') | (nation_df['N_NAME'] == 'INDIA')]

# Read supplier from MongoDB
supplier_df = pd.DataFrame(list(mongo_db.supplier.find()))
# Read customer from MongoDB
customer_df = pd.DataFrame(list(mongo_db.customer.find()))

# Prepare combined SQL to get suppliers/customers from Japan and India
suppliers_nation_df = supplier_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
customers_nation_df = customer_df.merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Get orders from Redis
orders_df = pd.read_json(redis_conn.get('orders'), orient='records')

# Get lineitems from Redis
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Filter by date
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] <= end_date)]

# Combine dataframes to compute the revenue volume
merged_df = (filtered_lineitem_df
             .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
             .merge(suppliers_nation_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
             .merge(customers_nation_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY'))

# Compute revenue
merged_df['revenue'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Filter for Japan and India pairs
filtered_revenue_df = merged_df[
    ((merged_df['N_NAME_x'] == 'JAPAN') & (merged_df['N_NAME_y'] == 'INDIA')) |
    ((merged_df['N_NAME_x'] == 'INDIA') & (merged_df['N_NAME_y'] == 'JAPAN'))
]

# Group by supplier nation, customer nation, and year of shipping
grouped_revenue = filtered_revenue_df.groupby(
    [filtered_revenue_df['N_NAME_x'],
     filtered_revenue_df['N_NAME_y'],
     filtered_revenue_df['L_SHIPDATE'].dt.year]
).agg({'revenue': 'sum'}).reset_index()

# Sort results
sorted_revenue = grouped_revenue.sort_values(by=['N_NAME_x', 'N_NAME_y', 'L_SHIPDATE'], ascending=True)

# Write to CSV
sorted_revenue.to_csv('query_output.csv', index=False)
