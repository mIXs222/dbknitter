import pymongo
import mysql.connector
import pandas as pd

# mysql connector
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

mycursor = mydb.cursor()
mycursor.execute("""
    SELECT
        N_NAME AS NATION,
        P_PARTKEY,
        S_SUPPKEY
    FROM 
        nation 
        INNER JOIN supplier ON nation.N_NATIONKEY = supplier.S_NATIONKEY
""")
mysql_data = mycursor.fetchall()

# save it to dataframe
df_mysql = pd.DataFrame(mysql_data, columns=['NATION', 'PARTKEY', 'SUPPKEY'])

# mongodb connector
myclient = pymongo.MongoClient("mongodb://mongodb:27017/")
mydb = myclient["tpch"]

# Collect data from partssup, orders and lineitem
partsup_collection = mydb["partsupp"]
df_partsup = pd.DataFrame(list(partsup_collection.find()))

orders_collection = mydb["orders"]
df_orders = pd.DataFrame(list(orders_collection.find()))

lineitem_collection = mydb["lineitem"]
df_lineitem = pd.DataFrame(list(lineitem_collection.find()))

# Merge all data
merged_df = pd.merge(df_mysql, df_partsup, how='inner', left_on=['PARTKEY', 'SUPPKEY'], 
                     right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
merged_df = pd.merge(merged_df, df_orders, how='inner', left_on='O_ORDERKEY', 
                     right_on='O_ORDERKEY')
merged_df = pd.merge(merged_df, df_lineitem, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], 
                     right_on=['L_PARTKEY', 'L_SUPPKEY'])

# Apply the query
merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - \
                      merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY']
merged_df['O_YEAR'] = pd.DatetimeIndex(merged_df['O_ORDERDATE']).year
grouped_df = merged_df.groupby(['NATION', 'O_YEAR']).agg({'AMOUNT': 'sum'}).reset_index()

# Save the result to file
grouped_df.to_csv('query_output.csv', index=False)
