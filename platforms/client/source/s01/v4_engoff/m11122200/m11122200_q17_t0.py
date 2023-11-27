import pymysql
import pymongo
import csv

# Connect to MySQL
connection_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

mysql_conn = pymysql.connect(**connection_params)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
part_collection = mongodb['part']

# Find parts from MongoDB - parts of a brand 23 and with MED BAG
part_filter = {'P_BRAND': 'Brand#23', 'P_CONTAINER': 'MED BAG'}
part_keys = part_collection.find(part_filter, {'P_PARTKEY': 1})

part_keys_list = [part['P_PARTKEY'] for part in part_keys]

# Query to find the average quantity ordered for these parts in MySQL
quantity_query = """
SELECT AVG(L_QUANTITY) as average_quantity
FROM lineitem
WHERE L_PARTKEY IN (%s)
"""

in_p = ', '.join(['%s'] * len(part_keys_list))  # Create placeholder for IN clause
mysql_cursor.execute(quantity_query % in_p, part_keys_list)

average_quantity_result = mysql_cursor.fetchone()
average_quantity = average_quantity_result[0]

# Now, calculate the average yearly gross loss in revenue
revenue_loss_query = """
SELECT SUM(L_EXTENDEDPRICE) / 7 as avg_yearly_loss
FROM lineitem
WHERE L_PARTKEY IN (%s) AND L_QUANTITY < (%s * 0.2)
"""

mysql_cursor.execute(revenue_loss_query % (in_p, '%s'), part_keys_list + [average_quantity])

avg_yearly_loss_result = mysql_cursor.fetchone()
avg_yearly_loss = avg_yearly_loss_result[0] if avg_yearly_loss_result[0] is not None else 0

# Write results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['average_yearly_loss'])
    writer.writerow([avg_yearly_loss])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
