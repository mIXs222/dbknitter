import mysql.connector
import pymongo
import pandas as pd

# Mysql connection
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)
mycursor = mydb.cursor()

# MongoDB connection
myclient = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = myclient["tpch"]

# Querying MySQL data
mycursor.execute("""
    SELECT * FROM nation
    UNION ALL
    SELECT * FROM part
    UNION ALL
    SELECT * FROM supplier
""")
mysql_data = mycursor.fetchall()

# Querying MongoDB data
partsupp_data = mongodb.partsupp.find()
orders_data = mongodb.orders.find()
lineitem_data = mongodb.lineitem.find()

# Converting to dataframes
mysql_df = pd.DataFrame(mysql_data)
partsupp_df = pd.DataFrame(partsupp_data)
orders_df = pd.DataFrame(orders_data)
lineitem_df = pd.DataFrame(lineitem_data)

# Merging dataframes
merged_df = pd.merge(partsupp_df, orders_df, on="_id", how="outer")
merged_df = pd.merge(merged_df, lineitem_df, on="_id", how="outer")
merged_df = pd.merge(merged_df, mysql_df, on="_id", how="outer")

# Generating profit column
merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE']*(1 - merged_df['L_DISCOUNT']) - merged_df['PS_SUPPLYCOST']*merged_df['L_QUANTITY']

# Grouping by nation and order year and computing total profit
grouped_df = merged_df.groupby(['NATION', 'O_YEAR'])['AMOUNT'].sum().reset_index()

# Renaming and sorting
grouped_df.rename(columns={'AMOUNT':'SUM_PROFIT'}, inplace=True)
grouped_df.sort_values(['NATION', 'O_YEAR'], ascending=[True,False], inplace=True)

# Writing to CSV
grouped_df.to_csv('query_output.csv', index=False)
