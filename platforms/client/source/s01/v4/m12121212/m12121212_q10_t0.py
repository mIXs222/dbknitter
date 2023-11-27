import pymongo
import pandas as pd
from datetime import datetime
import direct_redis
import csv

# Connection to mongodb
client = pymongo.MongoClient('mongodb', 27017)
db = client['tpch']

# Querying mongodb collections
nation_df = pd.DataFrame(list(db.nation.find()))
orders_df = pd.DataFrame(list(db.orders.find({
    "O_ORDERDATE": {"$gte": datetime(1993, 10, 1), "$lt": datetime(1994, 1, 1)}   
})))

# Connection to redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Reading in DataFrames from Redis
customer_df = pd.DataFrame(r.get('customer'))
lineitem_df = pd.DataFrame(r.get('lineitem'))

# Filtering the lineitem DataFrame
lineitem_df = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'R']

# Merging DataFrames
merged_df = customer_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = merged_df.merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculating REVENUE
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Grouping and aggregating the result
result_df = merged_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'], as_index=False)['REVENUE'].sum()

# Sorting the result
result_df = result_df.sort_values(by='REVENUE', ascending=[True, True, True, False])

# Writing the result to a CSV file
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)
