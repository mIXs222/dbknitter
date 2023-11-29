import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Execute MySQL query for lineitem data
lineitem_query = """
SELECT L_PARTKEY, L_QUANTITY, (L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS loss 
FROM lineitem
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(lineitem_query)
    lineitem_data = cursor.fetchall()

# Close MySQL connection
mysql_conn.close()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Query MongoDB for part data
part_data = list(mongodb_db.part.find(
    {"P_BRAND": "BRAND#23", "P_CONTAINER": "MED BAG"},
    {"_id": 0, "P_PARTKEY": 1}
))

# Close MongoDB connection
mongodb_client.close()

# Filter part keys from MongoDB
part_keys = [part['P_PARTKEY'] for part in part_data]

# Filter lineitem_data with part_keys
lineitem_data_filtered = [
    (partkey, quantity, loss) for partkey, quantity, loss in lineitem_data if partkey in part_keys
]

# Calculate the average quantity
average_quantity = sum(quantity for _, quantity, _ in lineitem_data_filtered) / len(lineitem_data_filtered)

# Calculate the average yearly gross loss
gross_loss = sum(loss for _, quantity, loss in lineitem_data_filtered if quantity < (0.2 * average_quantity))
average_yearly_gross_loss = gross_loss / 7  # Considering the 7-year database

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["average_yearly_gross_loss"])
    writer.writerow([average_yearly_gross_loss])

