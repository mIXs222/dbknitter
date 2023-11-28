# Python code to execute the query and write output to 'query_output.csv'

import pymysql
import pymongo
import pandas as pd

# MySQL connection
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetch data from MySQL
mysql_query = """
SELECT 
    L_PARTKEY,
    SUM(L_QUANTITY) AS TOTAL_QUANTITY,
    AVG(L_QUANTITY) AS AVG_QUANTITY,
    L_EXTENDEDPRICE
FROM lineitem
GROUP BY L_PARTKEY
"""
mysql_cursor.execute(mysql_query)
lineitem_data = mysql_cursor.fetchall()

# Store lineitem data in a DataFrame
lineitem_df = pd.DataFrame(lineitem_data, columns=['L_PARTKEY', 'TOTAL_QUANTITY', 'AVG_QUANTITY', 'L_EXTENDEDPRICE'])

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', port=27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Fetch data from MongoDB
part_data = list(part_collection.find({"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"}))

# Store part data to a DataFrame
part_df = pd.DataFrame(part_data)

# Data processing
# Only take necessary columns from part_df
part_key_quantities = part_df[['P_PARTKEY', 'P_RETAILPRICE']]

# Merge data based on part keys
merged_df = pd.merge(left=lineitem_df, right=part_key_quantities, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Applying the conditions: quantity less than 20% of AVG_QUANTITY
filtered_df = merged_df[merged_df['TOTAL_QUANTITY'] < 0.2 * merged_df['AVG_QUANTITY']]

# Calculating the average yearly extended price
filtered_df['AVG_YEARLY_EXTENDED_PRICE'] = filtered_df['L_EXTENDEDPRICE'] / 7.0

# Selecting the final output columns
output_df = filtered_df[['L_PARTKEY', 'AVG_YEARLY_EXTENDED_PRICE']]

# Write output to CSV file
output_df.to_csv('query_output.csv', index=False)

# Close the database connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
