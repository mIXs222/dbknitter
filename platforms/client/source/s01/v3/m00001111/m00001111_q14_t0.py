import mysql.connector
from pymongo import MongoClient
import pandas as pd
from datetime import datetime

# connect to Mysql
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch",
)

# select data from part table
mycursor = mydb.cursor()
mycursor.execute("SELECT * FROM part")
part_data = mycursor.fetchall()
part_df = pd.DataFrame(part_data, columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

# connect to MongoDB
client = MongoClient("mongodb:27017")
db = client.tpch

# select data from lineitem table
cursor = db.lineitem.find()
lineitem_data =  [li for li in cursor]
lineitem_df = pd.DataFrame(lineitem_data)
lineitem_df['L_SHIPDATE'] = lineitem_df['L_SHIPDATE'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))

# merge the dataframes
merged_df = pd.merge(lineitem_df, part_df, how="inner", left_on="L_PARTKEY", right_on="P_PARTKEY")

# filter data based on conditions
merged_df = merged_df[(merged_df['L_SHIPDATE'] >= '1995-09-01') & (merged_df['L_SHIPDATE'] < '1995-10-01')]

#calculation
merged_df['CALCULATION'] = merged_df.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']) if row['P_TYPE'].startswith('PROMO') else 0, axis=1)
PROMO_REVENUE = 100.00 * merged_df['CALCULATION'].sum() / (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])).sum()

# write to csv file
out_df=pd.DataFrame([PROMO_REVENUE], columns=['PROMO_REVENUE'])
out_df.to_csv('query_output.csv', index=False)
