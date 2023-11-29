# Filename: query.py
import pymysql
import pymongo
import csv

# Function to retrieve orders from MySQL
def get_mysql_orders():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch')
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT O_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, SUM(L_QUANTITY) AS TOTAL_QUANTITY
                FROM orders
                INNER JOIN lineitem ON O_ORDERKEY = L_ORDERKEY
                GROUP BY O_ORDERKEY
                HAVING SUM(L_QUANTITY) > 300
            """
            cursor.execute(query)
            rows = cursor.fetchall()
    finally:
        connection.close()
    return rows

# Function to retrieve customer names from MongoDB
def get_mongodb_customers():
    client = pymongo.MongoClient(host='mongodb', port=27017)
    db = client['tpch']
    customers = list(db.customer.find({}, {"C_CUSTKEY": 1, "C_NAME": 1, "_id": 0}))
    return {customer['C_CUSTKEY']: customer['C_NAME'] for customer in customers}

# Execute the query and write to CSV
def execute_query():
    orders = get_mysql_orders()
    customers = get_mongodb_customers()
    results = []

    for row in orders:
        if row[0] in customers:
            results.append({
                'customer_name': customers[row[0]],
                'customer_key': row[0],
                'order_key': row[1],
                'order_date': row[2],
                'total_price': row[3],
                'total_quantity': row[4],
            })

    results.sort(key=lambda x: (-x['total_price'], x['order_date']))

    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['customer_name', 'customer_key', 'order_key', 'order_date', 'total_price', 'total_quantity']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for result in results:
            writer.writerow(result)

if __name__ == '__main__':
    execute_query()
