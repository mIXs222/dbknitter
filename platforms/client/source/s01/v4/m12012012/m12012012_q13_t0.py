# query_execution.py
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

# Connect to Mongodb
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

def fetch_customers():
    with mysql_conn.cursor() as cursor:
        sql = "SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT FROM customer"
        cursor.execute(sql)
        return {row[0]: row for row in cursor.fetchall()}

def fetch_orders():
    orders_collection = mongodb_db['orders']
    return list(orders_collection.find(
        {'O_COMMENT': {'$not': pymongo.regex.Regex('pending%deposits%')}}
    ))

def execute_query(customers, orders):
    customer_to_ordercount = {cust[0]: 0 for cust in customers.values()}
    for order in orders:
        if order['O_CUSTKEY'] in customer_to_ordercount:
            customer_to_ordercount[order['O_CUSTKEY']] += 1

    count_to_numcustomers = {}
    for count in customer_to_ordercount.values():
        count_to_numcustomers.setdefault(count, 0)
        count_to_numcustomers[count] += 1

    results = sorted(
        ((count, num_customers) for count, num_customers in count_to_numcustomers.items()),
        key=lambda x: (-x[1], -x[0])
    )
    return results

def write_to_csv(data, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['C_COUNT', 'CUSTDIST'])
        writer.writerows(data)

def main():
    customers = fetch_customers()
    orders = fetch_orders()
    query_results = execute_query(customers, orders)
    write_to_csv(query_results, 'query_output.csv')

if __name__ == "__main__":
    main()
    mysql_conn.close()
    mongodb_client.close()
