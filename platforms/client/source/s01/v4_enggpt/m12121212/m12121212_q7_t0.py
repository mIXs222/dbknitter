import pymongo
import direct_redis
import pandas as pd
from datetime import datetime

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Function to normalize Redis data and convert to DataFrame
def redis_to_df(table_name):
    data = redis_client.get(table_name)
    df = pd.read_json(data)
    return df

# MongoDB queries for 'orders' and 'nation' tables
orders = pd.DataFrame(list(mongo_db.orders.find()))
nation = pd.DataFrame(list(mongo_db.nation.find()))

# Loading data from Redis for 'supplier', 'customer', and 'lineitem' tables
supplier_df = redis_to_df('supplier')
customer_df = redis_to_df('customer')
lineitem_df = redis_to_df('lineitem')

# Processing start
# Convert order date to DateTime
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])

# Filter lineitem for years 1995 and 1996
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem = lineitem_df[(lineitem_df['L_SHIPDATE'] >= datetime(1995, 1, 1)) & 
                                (lineitem_df['L_SHIPDATE'] <= datetime(1996, 12, 31))]

# Revenue calculation
filtered_lineitem['REVENUE'] = filtered_lineitem['L_EXTENDEDPRICE'] * (1 - filtered_lineitem['L_DISCOUNT'])

# Filtering nations for 'JAPAN' and 'INDIA'
nation = nation[nation['N_NAME'].isin(['JAPAN', 'INDIA'])]

# Merging datasets
merge1 = pd.merge(filtered_lineitem, orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merge2 = pd.merge(merge1, customer_df, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merge3 = pd.merge(merge2, supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
final_merge = pd.merge(merge3, nation, how='inner', left_on=['S_NATIONKEY', 'C_NATIONKEY'], right_on=['N_NATIONKEY', 'N_NATIONKEY'])

# Selecting required columns
result = final_merge[['N_NAME_x', 'N_NAME_y', 'L_SHIPDATE', 'REVENUE']]
result.rename(columns={'N_NAME_x': 'SUPPLIER_NATION', 'N_NAME_y': 'CUSTOMER_NATION', 'L_SHIPDATE': 'YEAR_OF_SHIPPING'}, inplace=True)
result['YEAR_OF_SHIPPING'] = result['YEAR_OF_SHIPPING'].dt.year

# Filtering for JAPAN and INDIA in supplier and customer nations
result = result[((result['SUPPLIER_NATION'] == 'JAPAN') & (result['CUSTOMER_NATION'] == 'INDIA')) | 
                ((result['SUPPLIER_NATION'] == 'INDIA') & (result['CUSTOMER_NATION'] == 'JAPAN'))]

# Grouping and sorting the results
grouped = result.groupby(['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR_OF_SHIPPING']).sum().reset_index()
sorted_grouped = grouped.sort_values(by=['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR_OF_SHIPPING'])

# Output to CSV
sorted_grouped.to_csv('query_output.csv', index=False)
