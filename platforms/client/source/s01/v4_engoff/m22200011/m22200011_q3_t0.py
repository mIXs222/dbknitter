import pymysql
import pymongo
import csv

def get_mysql_connection():
    return pymysql.connect(host='mysql',
                           user='root',
                           password='my-secret-pw',
                           database='tpch')

def get_mongodb_connection():
    client = pymongo.MongoClient('mongodb', 27017)
    return client['tpch']

def get_customer_data():
    customers = {}
    mysql_conn = get_mysql_connection()
    with mysql_conn.cursor() as cursor:
        cursor.execute("SELECT C_CUSTKEY, C_MKTSEGMENT FROM customer WHERE C_MKTSEGMENT='BUILDING';")
        for row in cursor.fetchall():
            customers[row[0]] = row[1]
    mysql_conn.close()
    return customers

def get_orders_and_lineitems(customers, benchmark_date):
    mongo_conn = get_mongodb_connection()
    orders_collection = mongo_conn['orders']
    lineitem_collection = mongo_conn['lineitem']

    filtered_orders = orders_collection.find(
                            {'O_ORDERDATE': {'$gt': benchmark_date}, 'O_CUSTKEY': {'$in': list(customers.keys())}},
                            {'_id': 0, 'O_ORDERKEY': 1, 'O_SHIPPRIORITY': 1}
                        )

    orders_and_revenue = []
    for order in filtered_orders:
        lineitems = lineitem_collection.find(
                        {'L_ORDERKEY': order['O_ORDERKEY'], 'L_SHIPDATE': {'$gt': benchmark_date}},
                        {'_id': 0, 'L_ORDERKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1}
                    )
        revenue = sum([li['L_EXTENDEDPRICE'] * (1 - li['L_DISCOUNT']) for li in lineitems])
        if revenue > 0:
            orders_and_revenue.append((order['O_ORDERKEY'], order['O_SHIPPRIORITY'], revenue))

    orders_and_revenue.sort(key=lambda x: x[2], reverse=True)
    return orders_and_revenue

def main():
    customers = get_customer_data()
    results = get_orders_and_lineitems(customers, '1995-03-15')
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['O_ORDERKEY', 'O_SHIPPRIORITY', 'REVENUE'])
        for row in results:
            csvwriter.writerow(row)

if __name__ == '__main__':
    main()
