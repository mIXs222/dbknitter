import pymysql
import pymongo
import csv

# MySQL Connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
lineitem_collection = mongodb['lineitem']

# Fetch parts with a specific brand from MySQL
mysql_cursor.execute("SELECT P_PARTKEY FROM part WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'")
part_keys = [result[0] for result in mysql_cursor.fetchall()]

# Close MySQL connection
mysql_conn.close()

# Calculate average quantity in line items from MongoDB
avg_quantity_pipeline = [
    {"$match": {
        "L_PARTKEY": {"$in": part_keys}
    }},
    {"$group": {
        "_id": None,
        "avg_quantity": {"$avg": "$L_QUANTITY"}
    }}
]
cursor = lineitem_collection.aggregate(avg_quantity_pipeline)
avg_quantity_result = list(cursor)
avg_quantity = avg_quantity_result[0]['avg_quantity'] if avg_quantity_result else 0

# Calculate the average yearly loss in revenue for parts with less quantity
loss_revenue_pipeline = [
    {"$match": {
        "L_PARTKEY": {"$in": part_keys},
        "L_QUANTITY": {"$lt": avg_quantity * 0.2}
    }},
    {"$group": {
        "_id": "$L_ORDERKEY",
        "loss_revenue": {"$sum": {"$multiply": ["$L_QUANTITY", "$L_EXTENDEDPRICE"]}}
    }}
]
cursor = lineitem_collection.aggregate(loss_revenue_pipeline)

# Calculate average yearly loss
total_loss_revenue = sum([doc['loss_revenue'] for doc in cursor])
years_in_dataset = 7  # Assuming data covers 7 years
avg_yearly_loss_revenue = total_loss_revenue / years_in_dataset

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['average_yearly_loss_revenue'])
    csvwriter.writerow([avg_yearly_loss_revenue])
