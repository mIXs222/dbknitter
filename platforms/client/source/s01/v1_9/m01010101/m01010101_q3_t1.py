import pymysql
import pymongo
import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine

# MySQL connection
mysql_conn = pymysql.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
# Execute SQL query
customer_query = "SELECT * FROM customer"
orders_query = "SELECT * FROM orders"
mysql_df1 = pd.read_sql(customer_query, mysql_conn)
mysql_df2 = pd.read_sql(orders_query, mysql_conn)

# MongoDB connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]

# Fetch data from MongoDB
lineitem_data = mongodb["lineitem"].find()
lineitem_df = pd.DataFrame(list(lineitem_data))

# Merge data
merged_df = pd.merge(mysql_df1, mysql_df2, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
final_df = pd.merge(merged_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Apply conditions
final_df = final_df[(final_df.C_MKTSEGMENT == 'BUILDING') &
                    (final_df.O_ORDERDATE < '1995-03-15') &
                    (final_df.L_SHIPDATE > '1995-03-15')]

# Calculate revenue
final_df['REVENUE'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])

# Group by L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
grouped_df = final_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).sum()

# Sort by REVENUE and O_ORDERDATE
grouped_df = grouped_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write output to csv
grouped_df.to_csv('query_output.csv')
