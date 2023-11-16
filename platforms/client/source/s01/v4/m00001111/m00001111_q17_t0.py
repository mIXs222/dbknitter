import pymysql
import pymongo
import pandas as pd

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Retrieve parts data from MySQL
part_query = """
SELECT P_PARTKEY, P_BRAND, P_CONTAINER
FROM part
WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'
"""
mysql_cursor.execute(part_query)
part_results = mysql_cursor.fetchall()
part_df = pd.DataFrame(list(part_results), columns=['P_PARTKEY', 'P_BRAND', 'P_CONTAINER'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
lineitem_collection = mongodb['lineitem']

# Retrieve lineitem data for the parts that match the condition from MongoDB
lineitem_df = pd.DataFrame(list(
    lineitem_collection.find({'L_PARTKEY': {'$in': part_df['P_PARTKEY'].tolist()}}, 
                             {'L_PARTKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_QUANTITY': 1})
))

# Calculate average quantity for each part
avg_qty_df = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_qty_df.rename(columns={'L_QUANTITY': 'AVG_QUANTITY'}, inplace=True)

# Merge part_df and lineitem_df on L_PARTKEY
combined_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
# Merge with average quantity
combined_df = pd.merge(combined_df, avg_qty_df, how='inner', on='L_PARTKEY')

# Apply the filter on quantity and calculate the result
filtered_df = combined_df[combined_df['L_QUANTITY'] < (0.2 * combined_df['AVG_QUANTITY'])]
result = filtered_df['L_EXTENDEDPRICE'].sum() / 7.0

# Output the result to a CSV file
output_df = pd.DataFrame([{'AVG_YEARLY': result}])
output_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
