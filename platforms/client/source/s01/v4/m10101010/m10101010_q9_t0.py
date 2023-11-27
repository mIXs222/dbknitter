import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Execute query for MySQL
with mysql_conn.cursor() as cursor:
    mysql_query = """
    SELECT
        S_NATIONKEY,
        L_EXTENDEDPRICE,
        L_DISCOUNT,
        L_QUANTITY,
        L_PARTKEY,
        L_SUPPKEY,
        L_ORDERKEY
    FROM
        supplier, lineitem
    WHERE
        S_SUPPKEY = L_SUPPKEY
    """
    cursor.execute(mysql_query)
    mysql_data = cursor.fetchall()

# Close MySQL connection
mysql_conn.close()

# Query MongoDB collections
nation = list(mongodb.nation.find({}, {'_id': 0}))
part = list(mongodb.part.find({'P_NAME': {'$regex': '.*dim.*'}}, {'_id': 0}))
partsupp = list(mongodb.partsupp.find({}, {'_id': 0}))
orders = list(mongodb.orders.find({}, {'_id': 0}))

# Convert MongoDB data to pandas dataframes
df_nation = pd.DataFrame(nation)
df_part = pd.DataFrame(part)
df_partsupp = pd.DataFrame(partsupp)
df_orders = pd.DataFrame(orders)

# Convert MySQL data to pandas dataframe
df_mysql_data = pd.DataFrame(mysql_data, columns=[
    'S_NATIONKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_QUANTITY', 'L_PARTKEY',
    'L_SUPPKEY', 'L_ORDERKEY'])

# Join the dataframes to mimic the SQL JOINs
merged_df = df_mysql_data.merge(df_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.merge(df_part, left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(df_partsupp, on=['L_PARTKEY', 'L_SUPPKEY'])
merged_df = merged_df.merge(df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Calculate 'AMOUNT' and cast O_ORDERDATE to datetime
merged_df['AMOUNT'] = merged_df.apply(lambda row: (row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT'])) -
                                                  (row['PS_SUPPLYCOST'] * row['L_QUANTITY']), axis=1)

merged_df['O_ORDERDATE'] = pd.to_datetime(merged_df['O_ORDERDATE'])
merged_df['O_YEAR'] = merged_df['O_ORDERDATE'].dt.year

# Group by NATION and O_YEAR, then calculate SUM_PROFIT
result_df = merged_df.groupby(['N_NAME', 'O_YEAR']).agg(SUM_PROFIT=('AMOUNT', 'sum')).reset_index()

# Sort the result dataframe
result_df = result_df.sort_values(by=['N_NAME', 'O_YEAR'], ascending=[True, False])

# Write the final result to a CSV
result_df.to_csv('query_output.csv', index=False)
