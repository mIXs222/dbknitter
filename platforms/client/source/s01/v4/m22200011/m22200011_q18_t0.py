import pymysql
import pymongo
import pandas as pd
import csv

# Establish a connection to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)
mysql_cursor = mysql_conn.cursor()

# Establish a connection to the MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get qualifying order keys from MongoDB with the sum of quantity > 300
pipeline = [
    {"$group": {"_id": "$L_ORDERKEY", "total_quantity": {"$sum": "$L_QUANTITY"}}},
    {"$match": {"total_quantity": {"$gt": 300}}},
    {"$project": {"L_ORDERKEY": "$_id", "_id": 0}}
]
qualifying_orders = list(mongo_db.lineitem.aggregate(pipeline))
qualifying_order_keys = [doc['L_ORDERKEY'] for doc in qualifying_orders]

# Construct the SQL query for MySQL
sql_query = """
SELECT
    C_NAME,
    C_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE,
    O_TOTALPRICE
FROM
    customer
JOIN
    orders ON C_CUSTKEY = O_CUSTKEY
WHERE
    O_ORDERKEY IN (%s)
""" % ','.join(['%s'] * len(qualifying_order_keys))

# Execute the SQL query
mysql_cursor.execute(sql_query, qualifying_order_keys)
mysql_results = mysql_cursor.fetchall()

# Convert MySQL results to Pandas DataFrame
mysql_df = pd.DataFrame(mysql_results, columns=['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])

# Sum quantities for each order in MongoDB
quantities_df = pd.DataFrame(qualifying_orders)

# Merge data from different DBMS
merged_data = pd.merge(
    mysql_df,
    quantities_df,
    left_on='O_ORDERKEY',
    right_on='L_ORDERKEY'
)

# Write the merged dataframe to a CSV file
output_columns = ['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'total_quantity']
merged_data.to_csv('query_output.csv', index=False, columns=output_columns)

# Close the database connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
