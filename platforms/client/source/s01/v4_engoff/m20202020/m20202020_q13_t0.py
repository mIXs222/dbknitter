import pymysql
import pandas as pd
import direct_redis
import csv

def connect_mysql(db_name, user, password, host):
    return pymysql.connect(host=host, user=user, password=password, db=db_name)

def connect_redis(hostname, port, db):
    return direct_redis.DirectRedis(host=hostname, port=port, db=db)

def execute_query():
    # Connect to MySQL
    mysql_connection = connect_mysql('tpch', 'root', 'my-secret-pw', 'mysql')

    # Read customer data from MySQL
    with mysql_connection.cursor() as cursor:
        cursor.execute("SELECT C_CUSTKEY FROM customer")
        customers = cursor.fetchall()
        
    # Map customers to their orders count
    customer_orders = {customer[0]: 0 for customer in customers}

    # Connect to Redis
    r = connect_redis('redis', 6379, 0)

    # Read orders data from Redis
    orders_df = pd.read_json(r.get('orders'))

    # Count orders for each customer
    for _, order in orders_df.iterrows():
        if order['O_ORDERSTATUS'] != "pending" and "deposits" not in order['O_COMMENT'].lower():
            if order['O_CUSTKEY'] in customer_orders:
                customer_orders[order['O_CUSTKEY']] += 1

    # Disconnect from databases
    mysql_connection.close()

    # Aggregate the results
    distribution_of_orders = pd.Series(customer_orders).value_counts().sort_index()

    # Write the output to query_output.csv
    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["number_of_orders", "number_of_customers"])
        for number_of_orders, number_of_customers in distribution_of_orders.items():
            writer.writerow([number_of_orders, number_of_customers])

execute_query()
