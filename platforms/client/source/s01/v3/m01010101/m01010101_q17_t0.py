import mysql.connector
import pandas as pd
from pymongo import MongoClient

# Establishing the connection to mysql
mysql_conn = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Firing the query to fetch "part" data from mysql
mysql_query = "SELECT * FROM part WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'"
mysql_cursor.execute(mysql_query)
part_data = mysql_cursor.fetchall()
part_df = pd.DataFrame(part_data, columns=[i[0] for i in mysql_cursor.description])
mysql_conn.close()

# Establishing the connection to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Firing the query to fetch "lineitem" data from mongodb
lineitem_data = list(mongodb.lineitem.find())
lineitem_df = pd.DataFrame(lineitem_data)

# Merging both dataframes on common key
merged_df = pd.merge(part_df, lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Function to calculate average quantity
def calculate_avg_quantity(l_partkey):
    avg_quantity = lineitem_df[lineitem_df['L_PARTKEY'] == l_partkey]['L_QUANTITY'].mean()
    return 0.2 * avg_quantity

merged_df['AVG_QUANTITY'] = merged_df['L_PARTKEY'].apply(lambda x: calculate_avg_quantity(x))
filtered_df = merged_df[merged_df['L_QUANTITY'] < merged_df['AVG_QUANTITY']]

# Calculating the final result
result = sum(filtered_df['L_EXTENDEDPRICE']) / 7.0

# Making a single row dataframe to write it to csv
output_df = pd.DataFrame({"AVG_YEARLY": [result]})
output_df.to_csv('query_output.csv', index=False)
