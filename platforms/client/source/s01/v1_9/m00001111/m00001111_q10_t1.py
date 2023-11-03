pip install pymssql pandas pymongo

# Python code
import pandas as pd
import MySQLdb
import pymongo
from bson.json_util import dumps

# Function to write data to csv
def write_to_csv(df, filename):
    df.to_csv(filename, index=False)

# Connecting to MySQL database 
db = MySQLdb.connect("mysql", "root", "my-secret-pw", "tpch")
cursor = db.cursor()

customer_query = '''SELECT C_CUSTKEY, C_NAME, C_ACCTBAL, C_PHONE, N_NAME, C_ADDRESS, C_COMMENT FROM customer, nation WHERE C_NATIONKEY = N_NATIONKEY'''
cursor.execute(customer_query)
customer_result = cursor.fetchall()

customer_df = pd.DataFrame(customer_result, columns=['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'])

# Connection to Mongo DB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = mongo_client["tpch"]

orders_cursor = db.orders.find({"O_ORDERDATE":{"$gte":"1993-10-01","$lt":"1994-01-01"}})
orders_df = pd.DataFrame(list(orders_cursor))

lineitem_cursor = db.lineitem.find({"L_RETURNFLAG":"R"})
lineitem_df = pd.DataFrame(list(lineitem_cursor))

merged_df = pd.merge(pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY'), lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
grouped_df = merged_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT']).sum().reset_index()

final_df = grouped_df.sort_values(['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])
write_to_csv(final_df, 'query_output.csv')
