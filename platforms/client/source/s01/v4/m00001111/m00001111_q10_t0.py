# query.py
import pymysql
import pymongo
import csv

# Connection information for MySQL
mysql_conn_info = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "database": "tpch",
}

# Connection information for MongoDB
mongodb_conn_info = {
    "host": "mongodb",
    "port": 27017,
    "database": "tpch",
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_info)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host=mongodb_conn_info["host"], port=mongodb_conn_info["port"])
mongodb = mongo_client[tpch_mongodb_conn_info["database"]]

# Query in MySQL for nation table
mysql_cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation;")
nation_data = mysql_cursor.fetchall()

# Mapping nation keys to names
nation_dict = {n[0]: n[1] for n in nation_data}

# Querying MongoDB for customers that match nation keys
customer_data = mongodb.customer.find({"C_NATIONKEY": {"$in": list(nation_dict.keys())}})
customer_dict = {(c["C_CUSTKEY"], c["C_NATIONKEY"]): c for c in customer_data}

# Querying MongoDB for the orders in the specified date range and L_RETURNFLAG
orders_match_stage = {
    "$match": {"O_ORDERDATE": {"$gte": "1993-10-01", "$lt": "1994-01-01"}}
}
orders_data = mongodb.orders.aggregate([orders_match_stage])

# Compiling the final result
results = []
for order in orders_data:
    customer_key = order["O_CUSTKEY"]
    order_key = order["O_ORDERKEY"]
    lineitems = mongodb.lineitem.find({"L_ORDERKEY": order_key, "L_RETURNFLAG": "R"})
    for lineitem in lineitems:
        customer = customer_dict.get((customer_key, order["O_CUSTKEY"]))
        if customer:
            revenue = lineitem["L_EXTENDEDPRICE"] * (1 - lineitem["L_DISCOUNT"])
            results.append([
                customer["C_CUSTKEY"],
                customer["C_NAME"],
                revenue,
                customer["C_ACCTBAL"],
                nation_dict[customer["C_NATIONKEY"]],
                customer["C_ADDRESS"],
                customer["C_PHONE"],
                customer["C_COMMENT"]
            ])

# Sort the results according to specified order in SQL query
results.sort(key=lambda x: (x[2], x[0], x[1], -x[3]))

# Write output to CSV file
with open('query_output.csv', mode='w', newline='') as output_file:
    writer = csv.writer(output_file)
    # Write header
    writer.writerow(["C_CUSTKEY", "C_NAME", "REVENUE", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT"])
    # Write data rows
    writer.writerows(results)

# Close the database connections
mysql_conn.close()
mongo_client.close()
