# python_code.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(
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

# Fetch nation data from Redis
nation_df = pd.read_json(redis_client.get('nation'))

# Perform SQL query on MySQL
mysql_query = """
SELECT
    l.L_ORDERKEY,
    l.L_EXTENDEDPRICE,
    l.L_DISCOUNT,
    l.L_SHIPDATE,
    l.L_SUPPKEY,
    o.O_CUSTKEY
FROM
    lineitem l
JOIN
    orders o ON l.L_ORDERKEY = o.O_ORDERKEY
WHERE
    l.L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31';
"""
lineitem_orders_df = pd.read_sql(mysql_query, mysql_connection)

# Close MySQL connection
mysql_connection.close()

# Fetch supplier and customer data from MongoDB
supplier_df = pd.DataFrame(list(mongo_db.supplier.find()))
customer_df = pd.DataFrame(list(mongo_db.customer.find()))

# Join MongoDB, Redis, and MySQL data
merged_df = (
    lineitem_orders_df
    .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(nation_df.rename(columns={'N_NATIONKEY': 'S_NATIONKEY', 'N_NAME': 'SUPP_NATION'}), on='S_NATIONKEY')
    .merge(nation_df.rename(columns={'N_NATIONKEY': 'C_NATIONKEY', 'N_NAME': 'CUST_NATION'}), on='C_NATIONKEY')
)

# Filtering for the conditions given in SQL query
filtered_df = merged_df[
    ((merged_df['SUPP_NATION'] == 'JAPAN') & (merged_df['CUST_NATION'] == 'INDIA')) |
    ((merged_df['SUPP_NATION'] == 'INDIA') & (merged_df['CUST_NATION'] == 'JAPAN'))
]

# Adding L_YEAR and VOLUME to the dataframe
filtered_df['L_YEAR'] = pd.to_datetime(filtered_df['L_SHIPDATE']).dt.year
filtered_df['VOLUME'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Grouping and summing VOLUME
output_df = (
    filtered_df
    .groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])
    .agg(REVENUE=('VOLUME', 'sum'))
    .reset_index()
    .sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])
)

# Write the output to a CSV file
output_df.to_csv('query_output.csv', index=False)
