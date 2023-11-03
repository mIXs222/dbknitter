from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
import pymysql

# MySQL connection
engine = create_engine('mysql+pymysql://root:my-secret-pw@mysql/tpch')
connection = engine.connect()

# querying MySQL
orders_query = pd.read_sql_query("SELECT * FROM ORDERS", connection)
nation_query = pd.read_sql_query("SELECT * FROM NATION", connection)

# MongoDb connection
client = MongoClient("mongodb://localhost:27017/")
db = client["tpch"]

# fetching data from MongoDb
customer_data = pd.DataFrame(list(db.customer.find({})))
lineitem_data = pd.DataFrame(list(db.lineitem.find({})))

# merging MySQL and MongoDb data
merged_data = pd.merge(orders_query, customer_data, left_on = 'O_CUSTKEY', right_on = 'C_CUSTKEY')
merged_data = pd.merge(merged_data, lineitem_data, left_on = 'O_ORDERKEY', right_on = 'L_ORDERKEY')

# applying the conditions from the SQL query
filtered_data = merged_data.loc[(merged_data['C_MKTSEGMENT'] == 'BUILDING') & 
                                (merged_data['O_ORDERDATE'] < '1995-03-15') & 
                                (merged_data['L_SHIPDATE'] > '1995-03-15')]

# calculating revenue
filtered_data['REVENUE'] = filtered_data['L_EXTENDEDPRICE'] * (1 - filtered_data['L_DISCOUNT'])

# selecting the columns needed for the final output
final_data = filtered_data[['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]

# sorting and grouping data
final_data = final_data.sort_values(by = ['REVENUE', 'O_ORDERDATE'], ascending = [False, True])
final_data = final_data.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'], as_index = False).sum()

# writing the output to a csv
final_data.to_csv('query_output.csv', index = False)

