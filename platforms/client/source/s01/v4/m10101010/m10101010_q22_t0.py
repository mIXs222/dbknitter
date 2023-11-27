import csv
import pymysql
import pymongo

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to the MongoDB database
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
orders_collection = mongodb_db['orders']

try:
    # Get average account balance from MySQL
    with mysql_conn.cursor() as cursor:
        cursor.execute(
            "SELECT AVG(C_ACCTBAL) FROM customer WHERE C_ACCTBAL > 0.00"
            " AND SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')"
        )
        result = cursor.fetchone()
        avg_acctbal = result[0]

    # Get customer data from MySQL
    with mysql_conn.cursor() as cursor:
        cursor.execute(
            "SELECT SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE, C_ACCTBAL, C_CUSTKEY "
            "FROM customer "
            "WHERE SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21') "
            "AND C_ACCTBAL > %s", (avg_acctbal,)
        )
        customers = cursor.fetchall()

    # Find customers with no orders in MongoDB
    filtered_customers = []
    for customer in customers:
        c_custkey = customer[2]
        order = orders_collection.find_one({'O_CUSTKEY': c_custkey})
        if order is None:
            filtered_customers.append(customer)

    # Aggregate customer data
    results = {}
    for customer in filtered_customers:
        cntrycode, acctbal, _ = customer
        if cntrycode not in results:
            results[cntrycode] = {'NUMCUST': 0, 'TOTACCTBAL': 0}
        results[cntrycode]['NUMCUST'] += 1
        results[cntrycode]['TOTACCTBAL'] += acctbal

    # Write results to CSV
    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for cntrycode, values in sorted(results.items()):
            writer.writerow({
                'CNTRYCODE': cntrycode,
                'NUMCUST': values['NUMCUST'],
                'TOTACCTBAL': values['TOTACCTBAL']
            })

finally:
    mysql_conn.close()
    mongodb_client.close()
