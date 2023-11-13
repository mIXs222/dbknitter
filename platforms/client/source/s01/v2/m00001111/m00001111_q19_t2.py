import mysql.connector
from pymongo import MongoClient
import pandas as pd

# Fetch data from MySQL
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

mycursor = mydb.cursor()
mycursor.execute("SELECT * FROM part")
mysql_data = mycursor.fetchall()
mysql_columns = mycursor.column_names
mysql_df = pd.DataFrame(mysql_data, columns=mysql_columns)

# Fetch data from MongoDB
client = MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]
col = mongodb["lineitem"]
mongo_data = list(col.find({}))
mongo_df = pd.DataFrame(mongo_data)

# Merge both dataframes
merged_df = pd.merge(mysql_df, mongo_df, how='inner', left_on='P_PARTKEY', right_on='L_PARTKEY')

# Apply the conditions and compute the output
condition1 = ((merged_df.P_BRAND == 'Brand#12') & (merged_df.P_CONTAINER.isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (merged_df.L_QUANTITY.between(1, 1 + 10)) & (merged_df.P_SIZE.between(1, 5)) & (merged_df.L_SHIPMODE.isin(['AIR', 'AIR REG'])) & (merged_df.L_SHIPINSTRUCT == 'DELIVER IN PERSON'))
condition2 = ((merged_df.P_BRAND == 'Brand#23') & (merged_df.P_CONTAINER.isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & (merged_df.L_QUANTITY.between(10, 10 + 10)) & (merged_df.P_SIZE.between(1, 10)) & (merged_df.L_SHIPMODE.isin(['AIR', 'AIR REG'])) & (merged_df.L_SHIPINSTRUCT == 'DELIVER IN PERSON'))
condition3 = ((merged_df.P_BRAND == 'Brand#34') & (merged_df.P_CONTAINER.isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (merged_df.L_QUANTITY.between(20, 20 + 10)) & (merged_df.P_SIZE.between(1, 15)) & (merged_df.L_SHIPMODE.isin(['AIR', 'AIR REG'])) & (merged_df.L_SHIPINSTRUCT == 'DELIVER IN PERSON'))

desired_df = merged_df[condition1 | condition2 | condition3]
desired_df['REVENUE'] = desired_df.loc[:, 'L_EXTENDEDPRICE'] * (1 - desired_df.loc[:, 'L_DISCOUNT'])

# Save output to csv
desired_df.to_csv('query_output.csv', index=False)
