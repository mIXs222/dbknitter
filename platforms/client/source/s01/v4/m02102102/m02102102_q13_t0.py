# Python code to execute the given query (query_execution.py)

import pymongo
import pymysql
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
customer_collection = mongo_db['customer']

# Fetch customer data from MongoDB
mongo_customers = customer_collection.find({}, {'_id': 0, 'C_CUSTKEY': 1})
customers = {doc['C_CUSTKEY']: [] for doc in mongo_customers}

# Execute the query for MySQL's 'orders' table
mysql_cursor.execute("""SELECT O_CUSTKEY, COUNT(O_ORDERKEY)
                        FROM orders
                        WHERE O_COMMENT NOT LIKE '%pending%deposits%'
                        GROUP BY O_CUSTKEY""")
orders_data = mysql_cursor.fetchall()

# Convert fetched orders data into a dictionary with customer key as the key
orders_dict = {}
for o_custkey, count_o_orderkey in orders_data:
    orders_dict[o_custkey] = count_o_orderkey

# Combine the data from MongoDB and MySQL in the customers dictionary
for c_custkey in customers:
    c_count = orders_dict.get(c_custkey, 0)
    customers[c_custkey].append(c_count)

# Group by C_COUNT and count customers (C_CUSTKEY)
c_count_distribution = {}
for c_custkey, c_counts in customers.items():
    c_count = c_counts[0]
    if c_count in c_count_distribution:
        c_count_distribution[c_count] += 1
    else:
        c_count_distribution[c_count] = 1

# Sort results according to the requirements
sorted_results = sorted(c_count_distribution.items(), key=lambda item: (-item[1], -item[0]))

# Write the final result to query_output.csv
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['C_COUNT', 'CUSTDIST'])
    for c_count, custdist in sorted_results:
        csvwriter.writerow([c_count, custdist])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
