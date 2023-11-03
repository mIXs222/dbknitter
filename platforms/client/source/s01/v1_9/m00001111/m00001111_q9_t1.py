import pymongo
import mysql.connector
from pymongo import MongoClient
import pandas as pd

# Connecting to MySQL database
cnx = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
cursor = cnx.cursor()

# executing command to fetch Nation, Supplier, Part Details
cursor.execute("SELECT * FROM NATION")
nation_data = cursor.fetchall()
nation_df = pd.DataFrame(nation_data, columns=["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"])

cursor.execute("SELECT * FROM SUPPLIER")
supplier_data = cursor.fetchall()
supplier_df = pd.DataFrame(supplier_data, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

cursor.execute("SELECT * FROM PART WHERE P_NAME LIKE '%dim%'")
part_data = cursor.fetchall()
part_df = pd.DataFrame(part_data, columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

# Connecting to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Fetch data from MongoDB
partsupp_data = pd.DataFrame(list(db.partsupp.find()))
lineitem_data = pd.DataFrame(list(db.lineitem.find()))
orders_data = pd.DataFrame(list(db.orders.find()))

# Merge all the dataframes based on the SQL conditions
merged_df = pd.merge(part_df, lineitem_data, left_on='P_PARTKEY', right_on='L_PARTKEY')
merged_df = pd.merge(merged_df, partsupp_data, left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
merged_df = pd.merge(merged_df, orders_data, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = pd.merge(merged_df, supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = pd.merge(merged_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculating Amount
merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY']

# Grouping by Nation and Year and calculating the SUM of Amount
result = merged_df.groupby(['N_NAME', merged_df['O_ORDERDATE'].dt.year]).agg({'AMOUNT': 'sum'}).reset_index()

# Sorting the result
result.sort_values(['N_NAME', 'O_ORDERDATE'], ascending=[True, False], inplace=True)

# writing the result to csv file
result.to_csv("query_output.csv", index=False)
