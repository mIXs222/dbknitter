import pymysql
import pymongo
from pymongo import MongoClient
import pandas as pd

# Create the MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch')

# Create the MongoDB connection
client = MongoClient('mongodb://mongodb:27017/')
mongo_db = client['tpch']

# Fetch data from MySQL tables
df_nation = pd.read_sql('SELECT * FROM NATION', con=mysql_conn)
df_customer = pd.read_sql('SELECT * FROM customer', con=mysql_conn)

# Fetch data from MongoDB collections
df_orders = pd.DataFrame(list(mongo_db.orders.find()))
df_lineitem = pd.DataFrame(list(mongo_db.lineitem.find()))

# Close connections
mysql_conn.close()
client.close()

# Merge dataframes as per the query's conditions
df = pd.merge(df_customer, df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df = pd.merge(df, df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df = pd.merge(df, df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Filter rows as per the query's conditions
df = df[(df['O_ORDERDATE'] >= '1993-10-01') & (df['O_ORDERDATE'] < '1994-01-01') & (df['L_RETURNFLAG'] == 'R')]

# Create the 'REVENUE' column
df['REVENUE'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])

# Group and sort as per the query's rules
df = df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT']).agg({'REVENUE': 'sum'})
df = df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])

# Write the output to a CSV file
df.to_csv('query_output.csv')
