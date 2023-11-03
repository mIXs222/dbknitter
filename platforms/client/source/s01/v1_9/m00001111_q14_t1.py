import pandas as pd
from pymongo import MongoClient
import mysql.connector

# Connection for MySQL
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# Connection for Mongo DB
client = MongoClient('mongodb://mongodb:27017/')
mongodb = client["tpch"]

mycursor = mydb.cursor()

# Query for MySQL
part_query = "SELECT * FROM part"
mycursor.execute(part_query)
part_data = mycursor.fetchall()

# Convert data to dataframe
part_df = pd.DataFrame(part_data, columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

# Get data from Mongo DB
lineitem_data = mongodb.lineitem.find()
lineitem_df = pd.DataFrame(list(lineitem_data))

# Restrict to required condition
part_df = part_df[part_df['P_TYPE'].str.contains('PROMO%')]
lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1995-09-01') & (lineitem_df['L_SHIPDATE'] < '1995-10-01')]

# Merge on 'L_PARTKEY' = 'P_PARTKEY'
merged_df = pd.merge(lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate Promo Revenue
merged_df['PROMO_REVENUE'] = merged_df.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']) if row['P_TYPE'].startswith('PROMO') else 0, axis=1)

# Sum of Promo Revenue
sum_promo_revenue = merged_df['PROMO_REVENUE'].sum()

# Sum of Total Revenue
sum_total_revenue = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])).sum()

# Percentage of Promo Revenue
promo_percentage = 100.00 * (sum_promo_revenue / sum_total_revenue)

# Write to .csv file
pd.DataFrame([promo_percentage], columns=["PROMO_REVENUE"]).to_csv('query_output.csv')
