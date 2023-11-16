import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient

# Connect to MySQL
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = conn.cursor()

# Get 'part' table from MySQL
mysql_cursor.execute("SELECT * FROM part")
p_keys = list(map(lambda x: x[0], mysql_cursor.description))
part_df = pd.DataFrame(mysql_cursor.fetchall(), columns=p_keys)

# Connect to MongoDB
client = MongoClient("mongodb", 27017)
mongodb = client['tpch']

# Get 'lineitem' table from MongoDB
result = list(mongodb.lineitem.find({}))
lineitem_df = pd.DataFrame(result)

# Merge dataframes
merged_df = pd.merge(part_df, lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Perform calculations to get PROMO_REVENUE
merged_df['L_SHIPDATE'] = pd.to_datetime(merged_df['L_SHIPDATE'])
merged_df = merged_df.loc[(merged_df['L_SHIPDATE'] >= '1995-09-01') & (merged_df['L_SHIPDATE'] < '1995-10-01')]
merged_df['promo_value'] = merged_df.apply(lambda x: x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']) if x['P_TYPE'].startswith('PROMO') else 0, axis=1)
promo_revenue = 100.00 * merged_df['promo_value'].sum() / (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])).sum()

# Write result to CSV
pd.DataFrame([promo_revenue], columns=['PROMO_REVENUE']).to_csv('query_output.csv', index=False)
