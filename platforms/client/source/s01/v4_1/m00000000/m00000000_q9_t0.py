import pandas as pd
import pymysql
from pymongo import MongoClient
import direct_redis

def get_df_from_sql(table_name, cursor):
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=table_name.columns())

# MySQL connection
connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor = connection.cursor()
mysql_tables = ['nation', 'part', 'supplier', 'partsupp', 'orders', 'lineitem']

# Get data from MySQL tables
mysql_data = {table: get_df_from_sql(table, cursor) for table in mysql_tables}

# MongoDB connection
mongo_client = MongoClient()
db = mongo_client.tpch
mongo_tables = ['nation', 'part', 'supplier', 'lineitem', 'orders']

# Get data from MongoDB tables
mongo_data = {table: pd.DataFrame(list(db[table].find())) for table in mongo_tables}

# Combine data from both databases
data = {**mysql_data, **mongo_data}

# Cleanup the connection
cursor.close()
connection.close()

# Run the query
query_df = data['part'].merge(data['supplier']).merge(data['lineitem']).merge(data['partsupp']).merge(data['orders']).merge(data['nation'])
query_df['O_YEAR'] = query_df['O_ORDERDATE'].str.slice(0, 4)
query_df['AMOUNT'] = query_df['L_EXTENDEDPRICE'] * (1 - query_df['L_DISCOUNT']) - query_df['PS_SUPPLYCOST'] * query_df['L_QUANTITY']
profit = query_df.loc[query_df['P_NAME'].str.contains('dim')].groupby(['N_NAME', 'O_YEAR'])['AMOUNT'].sum().reset_index()
profit.columns = ['NATION', 'O_YEAR', 'SUM_PROFIT']
profit.sort_values(['NATION', 'O_YEAR'], ascending=[True, False], inplace=True)

# Write output to CSV
profit.to_csv('query_output.csv', index=False)
