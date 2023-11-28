import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
mongo_customers = mongo_db["customer"]

# Fetch customer data from MongoDB
mongo_customers_data = list(mongo_customers.find({}, {"_id": 0, "C_CUSTKEY": 1}))

# Creating a list of customer keys
customer_keys_list = [data["C_CUSTKEY"] for data in mongo_customers_data]

# Placeholder for string format
format_strings = ','.join(['%s'] * len(customer_keys_list))

# MySQL query
mysql_query = """
SELECT C_ORDERS.C_CUSTKEY, COUNT(O_ORDERKEY) as O_COUNT
FROM (
    SELECT
        O_CUSTKEY
    FROM
        orders
    WHERE
        O_COMMENT NOT LIKE '%pending%'
        AND O_COMMENT NOT LIKE '%deposits%'
        AND O_CUSTKEY IN ({})
) as C_ORDERS
GROUP BY C_ORDERS.C_CUSTKEY;
""".format(format_strings)

mysql_cursor.execute(mysql_query, customer_keys_list)

orders_data = mysql_cursor.fetchall()

# Creating a dictionary to hold the count of orders per customer
customer_order_counts = {str(customer): 0 for customer in customer_keys_list}

# Filling the dictionary with actual order counts
for row in orders_data:
    customer_order_counts[str(row[0])] = row[1]

# Count the distribution
distribution = {}
for count in customer_order_counts.values():
    distribution.setdefault(count, 0)
    distribution[count] += 1

# Sort and write results to CSV
sorted_distribution = sorted(distribution.items(), key=lambda item: (-item[1], -item[0]))

with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['C_COUNT', 'CUSTDIST'])
    for count in sorted_distribution:
        writer.writerow(count)

# Closing connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
