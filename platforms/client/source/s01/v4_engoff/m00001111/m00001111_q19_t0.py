# query.py

import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client.tpch

# Query parts from MySQL
sql_query = """
    SELECT * FROM part WHERE 
    (P_BRAND = 'Brand#12' AND
    P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND
    P_SIZE BETWEEN 1 AND 5)
    OR
    (P_BRAND = 'Brand#23' AND
    P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND
    P_SIZE BETWEEN 1 AND 10)
    OR
    (P_BRAND = 'Brand#34' AND
    P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND
    P_SIZE BETWEEN 1 AND 15);
"""

parts_df = pd.read_sql(sql_query, con=mysql_conn)

# Query lineitem from MongoDB
mongo_query = {
    'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
    'L_SHIPINSTRUCT': 'DELIVER IN PERSON',
    '$or': [
        {'L_QUANTITY': {'$gte': 1, '$lte': 11}},
        {'L_QUANTITY': {'$gte': 10, '$lte': 20}},
        {'L_QUANTITY': {'$gte': 20, '$lte': 30}}
    ]
}
lineitems = mongodb.lineitem.find(mongo_query)
lineitems_df = pd.DataFrame(list(lineitems))

# Join the dataframes
result_df = pd.merge(parts_df, lineitems_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Calculate the gross discounted revenue
result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Select only required columns 
result_df = result_df[['L_ORDERKEY', 'REVENUE']]

# Write results to file
result_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
mongo_client.close()
