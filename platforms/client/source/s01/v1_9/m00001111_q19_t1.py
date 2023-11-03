import mysql.connector
import pandas as pd
from pymongo import MongoClient

# Connect to MySQL
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)
mycursor = mydb.cursor()

# Fetch data from part table
mycursor.execute("SELECT * FROM PART")
part_table = mycursor.fetchall()
part_df = pd.DataFrame(part_table, columns = ['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER',
                                              'P_RETAILPRICE', 'P_COMMENT'])

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_col = db['lineitem']

# Fetch data from lineitem collection
lineitem_data = lineitem_col.find()
lineitem_df = pd.DataFrame(list(lineitem_data))

# Merging lineitem & part DataFrame
merged_df = pd.merge(lineitem_df, part_df, left_on='L_PARTKEY', right_on="P_PARTKEY")

# Filtering data according to the given conditions
final_df = merged_df[((merged_df['P_BRAND'] == 'Brand#12') & (merged_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 
                                                                'SM PKG'])) & (merged_df['L_QUANTITY'] >= 1) & 
                      (merged_df['L_QUANTITY'] <= 11) & (merged_df['P_SIZE'].between(1, 5)) & 
                      (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & 
                      (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) | 
                     ((merged_df['P_BRAND'] == 'Brand#23') & (merged_df['P_CONTAINER'].isin(['MED BAG', 
                      'MED BOX', 'MED PKG', 'MED PACK'])) & (merged_df['L_QUANTITY'] >= 10) & 
                      (merged_df['L_QUANTITY'] <= 20) & (merged_df['P_SIZE'].between(1, 10)) & 
                      (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & 
                      (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) | 
                     ((merged_df['P_BRAND'] == 'Brand#34') & (merged_df['P_CONTAINER'].isin(['LG CASE', 
                      'LG BOX', 'LG PACK', 'LG PKG'])) & (merged_df['L_QUANTITY'] >= 20) & 
                      (merged_df['L_QUANTITY'] <= 30) & (merged_df['P_SIZE'].between(1, 15)) & 
                      (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & 
                      (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'))]
                     
# Calculate Revenue
final_df['REVENUE'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])
output = final_df['REVENUE'].sum()

# Writing data to CSV
output_df = pd.DataFrame([output], columns=['REVENUE'])
output_df.to_csv('query_output.csv', index=False)
