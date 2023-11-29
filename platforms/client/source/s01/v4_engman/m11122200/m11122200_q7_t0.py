# query_executor.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Query to select records from lineitem and orders in MySQL
mysql_query = """
    SELECT
        O_CUSTKEY,
        L_SUPPKEY,
        L_EXTENDEDPRICE,
        L_DISCOUNT,
        O_ORDERDATE
    FROM
        lineitem
    INNER JOIN orders ON L_ORDERKEY = O_ORDERKEY
    WHERE
        O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31';
"""

with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    lineitem_orders_result = cursor.fetchall()

# Convert MySQL data to DataFrame
lineitem_orders_df = pd.DataFrame(lineitem_orders_result,
                                  columns=['O_CUSTKEY', 'L_SUPPKEY', 'L_EXTENDEDPRICE',
                                           'L_DISCOUNT', 'O_ORDERDATE'])

# Calculate year and revenue
lineitem_orders_df['L_YEAR'] = lineitem_orders_df['O_ORDERDATE'].apply(lambda x: x.year)
lineitem_orders_df['REVENUE'] = lineitem_orders_df.apply(
    lambda x: x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']), axis=1)

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client.tpch
nation_collection = mongodb.nation

# Query for nations in MongoDB
nation_query = {'N_NAME': {'$in': ['INDIA', 'JAPAN']}}
nations = list(nation_collection.find(nation_query))

# Extracting national data into a DataFrame
nation_df = pd.DataFrame(nations)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
supplier_df = pd.read_json(redis_client.get('supplier'))
customer_df = pd.read_json(redis_client.get('customer'))

# Filter suppliers and customers from India and Japan
suppliers_to_include = nation_df.loc[nation_df['N_NAME'] == 'INDIA', 'N_NATIONKEY'].tolist() + \
                       nation_df.loc[nation_df['N_NAME'] == 'JAPAN', 'N_NATIONKEY'].tolist()

supplier_df_filtered = supplier_df[supplier_df['S_NATIONKEY'].isin(suppliers_to_include)]
customer_df_filtered = customer_df[customer_df['C_NATIONKEY'].isin(suppliers_to_include)]

# Merge dataframes based on suppliers and customers
merged_df = lineitem_orders_df.merge(supplier_df_filtered, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
final_df = merged_df.merge(customer_df_filtered, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Finalize the DataFrame we need for the query output
final_query_df = final_df[[
    'C_NATIONKEY', 'L_YEAR', 'REVENUE', 'S_NATIONKEY'
]].copy()

# Mapping Nation Keys back to Nation Names
nation_dict = nation_df.set_index('N_NATIONKEY')['N_NAME'].to_dict()
final_query_df['CUST_NATION'] = final_query_df['C_NATIONKEY'].map(nation_dict)
final_query_df['SUPP_NATION'] = final_query_df['S_NATIONKEY'].map(nation_dict)

# Filtering based on supplier and customer nations
final_query_df = final_query_df.query(
    "(CUST_NATION == 'INDIA' and SUPP_NATION == 'JAPAN') or (CUST_NATION == 'JAPAN' and SUPP_NATION == 'INDIA')"
)

# Selecting and renaming columns for final output
final_query_df = final_query_df[['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']]

# Sorting the results
final_query_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], inplace=True)

# Write to CSV
final_query_df.to_csv('query_output.csv', index=False)

# Close all connections
mysql_connection.close()
mongo_client.close()
redis_client.connection_pool.disconnect()
