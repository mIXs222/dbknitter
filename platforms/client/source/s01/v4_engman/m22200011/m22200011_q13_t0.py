import pymysql
import pymongo
import csv

# Function to fetch MySQL data
def fetch_mysql_data():
    connection = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        database='tpch',
    )

    try:
        with connection.cursor() as cursor:
            sql = "SELECT C_CUSTKEY FROM customer"
            cursor.execute(sql)
            result = cursor.fetchall()
            mysql_data = {row[0] for row in result}
    finally:
        connection.close()

    return mysql_data

# Function to fetch MongoDB data
def fetch_mongodb_data(customer_keys):
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    orders = db['orders']
    pipeline = [
        {"$match": {
            "O_CUSTKEY": {"$in": list(customer_keys)},
            "O_ORDERSTATUS": {"$ne": 'P'},  # Excluding pending orders
            "O_COMMENT": {"$not": pymongo.regex.Regex(".*pending.*deposits.*")}
        }},
        {"$group": {
            "_id": "$O_CUSTKEY",
            "count": {"$sum": 1}
        }}
    ]
    result = list(orders.aggregate(pipeline))
    client.close()

    return result

# Process data and write to CSV
def process_data():
    mysql_data = fetch_mysql_data()
    mongodb_data = fetch_mongodb_data(mysql_data)

    customer_order_count = {}
    for order in mongodb_data:
        count = order['count']
        if count in customer_order_count:
            customer_order_count[count] += 1
        else:
            customer_order_count[count] = 1

    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Number of Orders', 'Number of Customers'])
        for num_orders, num_customers in sorted(customer_order_count.items()):
            writer.writerow([num_orders, num_customers])

if __name__ == "__main__":
    process_data()
