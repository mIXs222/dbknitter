from pymongo import MongoClient
import pandas as pd

#  MongoDB connection info
hostname = "mongodb"
port = 27017
dbname = "tpch"

# Connect to the mongoDB
client = MongoClient(hostname, port)
db = client[dbname]

# Query and Load data into dataframes
df_nation = pd.DataFrame(list(db.nation.find({})))
df_part = pd.DataFrame(list(db.part.find({})))
df_supplier = pd.DataFrame(list(db.supplier.find({})))
df_partsupp = pd.DataFrame(list(db.partsupp.find({})))
df_orders = pd.DataFrame(list(db.orders.find({})))
df_lineitem = pd.DataFrame(list(db.lineitem.find({})))

# Preprocessing
df_nation['N_NATIONKEY'] = df_nation['_id']
df_orders['O_YEAR'] = df_orders['O_ORDERDATE'].dt.year
df_lineitem['AMOUNT'] = df_lineitem['L_EXTENDEDPRICE'] * (1 - df_lineitem['L_DISCOUNT']) - df_partsupp['PS_SUPPLYCOST'] * df_lineitem['L_QUANTITY']

# Merge the dataframes
merged_df = pd.merge(df_nation, df_supplier, how ='inner', left_on ='N_NATIONKEY', right_on ='S_NATIONKEY')
merged_df = pd.merge(merged_df, df_part, how ='inner', left_on ='P_PARTKEY', right_on ='P_PARTKEY')
merged_df = pd.merge(merged_df, df_orders, how ='inner', left_on ='O_ORDERKEY', right_on ='L_ORDERKEY')
merged_df = pd.merge(merged_df, df_lineitem, how ='inner', left_on ='L_ORDERKEY', right_on ='L_ORDERKEY')

# Query
result = merged_df[merged_df['N_NAME'].str.contains('dim')].groupby(['N_NAME','O_YEAR'])['AMOUNT'].sum().reset_index()

# Sorting
result = result.sort_values(by=['N_NAME', 'O_YEAR'], ascending=[True, False])

# Write to csv file
result.to_csv("query_output.csv", index = False)
