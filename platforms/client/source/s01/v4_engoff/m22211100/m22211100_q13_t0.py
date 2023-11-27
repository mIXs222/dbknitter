# query.py
import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')
mysql_cursor = mysql_connection.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['customer']

# Create function to get distinct customer counts from MySQL
def get_order_counts():
    query = """
    SELECT O_CUSTKEY, COUNT(*)
    FROM orders
    WHERE O_ORDERSTATUS NOT IN ('pending', 'deposits')
    GROUP BY O_CUSTKEY;
    """
    mysql_cursor.execute(query)
    return dict(mysql_cursor.fetchall())

# Get the customers with their order counts
order_counts = get_order_counts()

# Create a dictionary to store distribution of customers by number of orders
distribution = {}

# Initialize distribution with customers from MongoDB with zero orders
for customer in mongo_collection.find({}, {'C_CUSTKEY': 1}):
    custkey = customer['C_CUSTKEY']
    distribution[custkey] = distribution.get(custkey, 0)

# Update distribution with order counts
for custkey, count in order_counts.items():
    distribution[custkey] = count

# Count the distribution
final_distribution = {}
for count in distribution.values():
    final_distribution[count] = final_distribution.get(count, 0) + 1

# Write the query result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Number of Orders', 'Number of Customers'])
    for orders, customers in sorted(final_distribution.items()):
        writer.writerow([orders, customers])

# Close connections
mysql_cursor.close()
mysql_connection.close()
mongo_client.close()
