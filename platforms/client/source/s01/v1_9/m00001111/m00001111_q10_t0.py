from mysql.connector import connect
import pandas as pd
from pymongo import MongoClient

# MySQL connection 
config = {
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
    'database': 'tpch',
    'raise_on_warnings': True
}

mysql_conn = connect(**config)

# Fetching the MySQL tables
nation = pd.read_sql("SELECT * FROM NATION", mysql_conn)
customer = pd.read_sql("SELECT * FROM CUSTOMER", mysql_conn)

# MongoDB connection
client = MongoClient('mongodb', 27017)
db = client.tpch

# Fetching the MongoDB collections
orders = pd.DataFrame(list(db.orders.find()))
lineitem = pd.DataFrame(list(db.lineitem.find()))

# Merge all tables based on the conditions of the SQL query
result = (customer.merge(nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
                   .merge(orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
                   .merge(lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY'))

# Filter data
result = result.loc[(result['O_ORDERDATE'] >= '1993-10-01') & 
                    (result['O_ORDERDATE'] < '1994-01-01') & 
                    (result['L_RETURNFLAG'] == 'R')]

# Add revenue computed column
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Generate final result as requested in the SQL query
final_result = result.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT']).agg({'REVENUE': 'sum'})
final_result = final_result.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False])

# Write the final result to a csv file
final_result.to_csv('query_output.csv')
