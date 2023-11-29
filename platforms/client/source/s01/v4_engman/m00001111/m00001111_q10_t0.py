import pymongo
import pymysql
import csv
from datetime import datetime

# Establish connections
# Connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query MongoDB for lineitem and orders data
start_date = datetime(1993, 10, 1)
end_date = datetime(1994, 1, 1)
lineitem_query = {
    "L_SHIPDATE": {"$gte": start_date, "$lt": end_date},
    "L_RETURNFLAG": "R",
}
project_fields = {
    "L_ORDERKEY": 1,
    "L_EXTENDEDPRICE": 1,
    "L_DISCOUNT": 1,
}
lineitems = mongo_db['lineitem'].find(lineitem_query, project_fields)

# OrderKeys from lineitems for MongoDB order query
order_keys = [lineitem['L_ORDERKEY'] for lineitem in lineitems]
orders_query = {"O_ORDERKEY": {"$in": order_keys}}
orders = mongo_db['orders'].find(orders_query, {"O_CUSTKEY": 1})

# Extracting matched customer keys
customer_keys = [order['O_CUSTKEY'] for order in orders]

# Query MySQL for customer data
with mysql_conn.cursor() as cursor:
    query_customer = """
        SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_COMMENT
        FROM tpch.customer
        WHERE C_CUSTKEY IN (%s);
    """
    format_strings = ','.join(['%s'] * len(customer_keys))
    cursor.execute(query_customer % format_strings, tuple(customer_keys))

    # Fetch customer data from MySQL
    customers = cursor.fetchall()


# Combine data
def calculate_revenue_loss(lineitems):
    return sum(item['L_EXTENDEDPRICE'] * (1 - item['L_DISCOUNT']) for item in lineitems)

# Fetch the lineitem data again as the initial cursor would have been exhausted.
lineitems = mongo_db['lineitem'].find(lineitem_query, project_fields)
results = []
for cust in customers:
    # Filter lineitems by customer key
    cust_lineitems = filter(lambda l: l['L_ORDERKEY'] in order_keys, lineitems)
    revenue_lost = calculate_revenue_loss(cust_lineitems)
    results.append({
        'customer_key': cust[0],
        'customer_name': cust[1],
        'revenue_lost': revenue_lost,
        'account_balance': cust[5],
        'nation': cust[3],
        'address': cust[2],
        'phone': cust[4],
        'comment': cust[6],
    })

# Sort the results
sorted_results = sorted(results,
                        key=lambda x: (x['revenue_lost'], x['customer_key'], x['customer_name']),
                        reverse=True)

# Write the results to a CSV file
csv_columns = ['customer_key', 'customer_name', 'revenue_lost', 'account_balance', 'nation', 'address', 'phone', 'comment']
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    for data in sorted_results:
        writer.writerow(data)

# Close connections
mysql_conn.close()
mongo_client.close()
