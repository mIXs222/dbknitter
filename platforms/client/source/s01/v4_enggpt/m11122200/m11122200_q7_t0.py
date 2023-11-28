import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Query MySQL for orders and lineitem data
with mysql_conn.cursor() as cursor:
    query = """
    SELECT
        l.L_ORDERKEY,
        o.O_CUSTKEY,
        l.L_EXTENDEDPRICE,
        l.L_DISCOUNT,
        YEAR(l.L_SHIPDATE) AS year,
        l.L_SHIPDATE
    FROM
        lineitem l
    JOIN
        orders o
    ON
        l.L_ORDERKEY = o.O_ORDERKEY
    WHERE
        l.L_SHIPDATE >= '1995-01-01' AND l.L_SHIPDATE <= '1996-12-31'
    """
    cursor.execute(query)
    orders_lineitem_data = cursor.fetchall()

# Convert MySQL data to DataFrame
orders_lineitem_df = pd.DataFrame(orders_lineitem_data, columns=['L_ORDERKEY', 'O_CUSTKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'year', 'L_SHIPDATE'])

mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
nation_collection = mongo_db['nation']

# Query MongoDB for nation data
nation_data = list(nation_collection.find({'N_NAME': {'$in': ['JAPAN', 'INDIA']}}))
nation_df = pd.DataFrame(nation_data)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query Redis for supplier and customer data
supplier_df = pd.read_msgpack(redis_client.get('supplier'))
customer_df = pd.read_msgpack(redis_client.get('customer'))

# Filter nations in Redis DataFrames
supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df['N_NATIONKEY'])]
customer_df = customer_df[customer_df['C_NATIONKEY'].isin(nation_df['N_NATIONKEY'])]

# Merge dataframes to create the report
merged_df = pd.merge(orders_lineitem_df, customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = pd.merge(merged_df, supplier_df, left_on='L_ORDERKEY', right_on='S_SUPPKEY')
merged_df = pd.merge(merged_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
merged_df = pd.merge(merged_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY', suffixes=('_CUSTOMER', '_SUPPLIER'))
merged_df['revenue'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Filter for the years and nation conditions
filtered_df = merged_df[(merged_df['N_NAME_SUPPLIER'].isin(['JAPAN', 'INDIA'])) &
                        (merged_df['N_NAME_CUSTOMER'].isin(['JAPAN', 'INDIA'])) &
                        (~merged_df['N_NAME_SUPPLIER'].eq(merged_df['N_NAME_CUSTOMER'])) &
                        (merged_df['year'].isin([1995, 1996]))]

# Group by the required fields and calculate revenue
report_df = filtered_df.groupby(['N_NAME_SUPPLIER', 'N_NAME_CUSTOMER', 'year']).agg({'revenue': 'sum'}).reset_index()

# Sort the result
report_df.sort_values(by=['N_NAME_SUPPLIER', 'N_NAME_CUSTOMER', 'year'], inplace=True)

# Save output to CSV
report_df.to_csv('query_output.csv', index=False)
