import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Execute MySQL Query to get customers data
mysql_query = """
    SELECT DISTINCT C_CUSTKEY, C_NAME
    FROM customer
"""
mysql_cursor.execute(mysql_query)
customers_data = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Execute MongoDB Query to get orders and lineitems data
order_keys_with_total_qty_over_300 = mongodb_db['lineitem'].aggregate([
    {
        "$group": {
            "_id": "$L_ORDERKEY",
            "total_quantity": {
                "$sum": "$L_QUANTITY"
            }
        }
    },
    {
        "$match": {
            "total_quantity": {
                "$gt": 300
            }
        }
    }
])

order_keys_valid = [doc["_id"] for doc in order_keys_with_total_qty_over_300]

orders_data = list(mongodb_db['orders'].find(
    {
        'O_ORDERKEY': {'$in': order_keys_valid}
    },
    {
        'O_ORDERKEY': 1,
        'O_CUSTKEY': 1,
        'O_ORDERDATE': 1,
        'O_TOTALPRICE': 1
    }
))

# Join orders with customers
output_data = []
for order in orders_data:
    custkey = order['O_CUSTKEY']
    custname = customers_data.get(custkey)
    if custname:
        output_data.append((
            custkey,
            custname,
            order['O_ORDERKEY'],
            order['O_ORDERDATE'],
            order['O_TOTALPRICE']
        ))

# Sort the results based on O_TOTALPRICE and then by O_ORDERDATE
sorted_output = sorted(output_data, key=lambda x: (-x[4], x[3]))

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['C_CUSTKEY', 'C_NAME', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])
    for row in sorted_output:
        csvwriter.writerow(row)

# Cleanup
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
