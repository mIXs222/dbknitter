import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection
client = pymongo.MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch necessary data from MySQL
nation_query = "SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME IN ('INDIA', 'JAPAN');"
supplier_query = "SELECT S_SUPPKEY, S_NATIONKEY FROM supplier;"

with mysql_conn.cursor() as cursor:
    cursor.execute(nation_query)
    nations = cursor.fetchall()

    cursor.execute(supplier_query)
    suppliers = cursor.fetchall()

# Fetch necessary data from MongoDB
customers = list(mongodb.customer.find({}, {'C_CUSTKEY': 1, 'C_NATIONKEY': 1}))

# Creating DataFrames from fetched data
df_nations = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME'])
df_suppliers = pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'S_NATIONKEY'])
df_customers = pd.DataFrame(customers)

# Fetch lineitems from Redis
lineitems = redis_conn.get('lineitem')
df_lineitems = pd.read_json(lineitems)

# Merge DataFrames to filter relevant rows
supplier_nation_df = df_nations.merge(df_suppliers, left_on='N_NATIONKEY', right_on='S_NATIONKEY')
customer_nation_df = df_nations.merge(df_customers, left_on='N_NATIONKEY', right_on='C_NATIONKEY')

# Join with lineitems
result_df = df_lineitems.merge(supplier_nation_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY') \
                        .merge(customer_nation_df, left_on='L_ORDERKEY', right_on='C_CUSTKEY')

# Filter lineitems by date and nation conditions
result_df['L_SHIPDATE'] = pd.to_datetime(result_df['L_SHIPDATE'])
result_df = result_df[
    (result_df['L_SHIPDATE'].dt.year.isin([1995, 1996])) &
    ((result_df['N_NAME_x'].isin(['INDIA', 'JAPAN'])) & (result_df['N_NAME_y'].isin(['INDIA', 'JAPAN'])) & (result_df['N_NAME_x'] != result_df['N_NAME_y']))
]

# Calculate revenue
result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])
result_df['L_YEAR'] = result_df['L_SHIPDATE'].dt.year

# Group by the required fields and compute sum of revenue
grouped_result = result_df.groupby(['N_NAME_y', 'L_YEAR', 'N_NAME_x'])['REVENUE'].sum().reset_index()

# Rename columns to match the required output
grouped_result.columns = ['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']

# Sort the results
ordered_result = grouped_result.sort_values(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Write the output to CSV file
ordered_result.to_csv('query_output.csv', index=False)

# Close the database connection
mysql_conn.close()
client.close()
