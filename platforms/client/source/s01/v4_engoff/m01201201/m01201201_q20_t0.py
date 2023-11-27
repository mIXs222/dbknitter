# importing libraries
import pymysql
import pymongo
import pandas as pd
import csv

# Connect to the MySQL database
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

# Create a cursor object using the cursor() method
cursor = mysql_conn.cursor()

# Query to get suppliers and nations for CANADA
supplier_query = """
SELECT s.S_SUPPKEY, s.S_NAME
FROM supplier s
JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE n.N_NAME = 'CANADA';
"""
# Execute the query
cursor.execute(supplier_query)

# Fetch all rows
suppliers_in_canada = cursor.fetchall()

# Get suppliers as a dataframe
suppliers_df = pd.DataFrame(suppliers_in_canada, columns=["S_SUPPKEY", "S_NAME"])

# Close the MySQL cursor and connection
cursor.close()
mysql_conn.close()

# Connect to the MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get 'partsupp' and 'lineitem' collections
partsupp_collection = mongo_db['partsupp']
lineitem_collection = mongo_db['lineitem']

# Query to get part supplies for the given date range and join with the line items
pipeline = [
    {"$lookup": {
        "from": "lineitem",
        "localField": "PS_SUPPKEY",
        "foreignField": "L_SUPPKEY",
        "as": "lineitems"
    }},
    {"$unwind": "$lineitems"},
    {"$match": {
        "lineitems.L_SHIPDATE": {"$gte": "1994-01-01", "$lt": "1995-01-01"},
        "lineitems.L_PARTKEY": {"$regex": ".*forest.*"}  # Assuming 'forest' naming convention
    }},
    {"$group": {
        "_id": "$PS_SUPPKEY",
        "total_quantity": {"$sum": "$PS_AVAILQTY"}
    }}
]

# Execute the query
part_suppliers = list(partsupp_collection.aggregate(pipeline))

# Convert to DataFrame
part_suppliers_df = pd.DataFrame(part_suppliers)

# Merge the dataframes to get the suppliers who have an excess of a forest part
final_df = pd.merge(suppliers_df, part_suppliers_df, left_on="S_SUPPKEY", right_on="_id")
final_df['excess'] = final_df['total_quantity'] > final_df['total_quantity'] * 0.5
excess_suppliers = final_df[final_df['excess']]

# Drop unnecessary columns and rename the columns
excess_suppliers = excess_suppliers.drop(columns=['_id', 'excess'])
excess_suppliers.columns = ['SUPPLIER_KEY', 'SUPPLIER_NAME', 'TOTAL_QUANTITY']

# Write the output to CSV
excess_suppliers.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
