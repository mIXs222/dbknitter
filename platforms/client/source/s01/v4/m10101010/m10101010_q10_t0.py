import pymysql
import pymongo
import csv
from decimal import Decimal

# Connection information
mysql_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

mongo_config = {
    'host': 'mongodb',
    'port': 27017,
    'database': 'tpch'
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_config)
mysql_cur = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient(**mongo_config)
mongo_db = mongo_client[mongo_config['database']]

# Fetch data from MySQL
mysql_cur.execute("""
SELECT
    customer.C_CUSTKEY,
    customer.C_NAME,
    SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) AS REVENUE,
    customer.C_ACCTBAL,
    customer.C_ADDRESS,
    customer.C_PHONE,
    customer.C_COMMENT
FROM
    customer
JOIN
    lineitem ON customer.C_CUSTKEY = lineitem.L_ORDERKEY
WHERE
    lineitem.L_RETURNFLAG = 'R'
GROUP BY
    customer.C_CUSTKEY
""")

mysql_results = {
    row[0]: row[1:] for row in mysql_cur.fetchall()  # Maps C_CUSTKEY to the result row
}

# Fetch data from MongoDB
orders = mongo_db.orders.find({
    'O_ORDERDATE': {'$gte': '1993-10-01', '$lt': '1994-01-01'}
})

# Map C_CUSTKEY to O_ORDERKEY and get the unique C_CUSTKEYs to join with nation
order_keys_by_custkey = {}
for order in orders:
    order_keys_by_custkey[order['O_CUSTKEY']] = order['O_ORDERKEY']

nation = mongo_db.nation.find({})
nation_by_nationkey = {
    n['N_NATIONKEY']: n['N_NAME'] for n in nation
}

# Combine results from MySQL and MongoDB
final_results = []
for custkey, values in mysql_results.items():
    orderkey = order_keys_by_custkey.get(custkey)
    if orderkey:
        revenue = str(values[0]) if isinstance(values[0], Decimal) else values[0]
        acctbal = str(values[1]) if isinstance(values[1], Decimal) else values[1]
        address = values[2]
        phone = values[3]
        comment = values[4]

        nationname = nation_by_nationkey.get(custkey)
        if nationname:
            final_results.append({
                'C_CUSTKEY': custkey,
                'C_NAME': values[:1],
                'REVENUE': revenue,
                'C_ACCTBAL': acctbal,
                'N_NAME': nationname,
                'C_ADDRESS': address,
                'C_PHONE': phone,
                'C_COMMENT': comment
            })

# Close MySQL cursor and connection
mysql_cur.close()
mysql_conn.close()

# Sort the final results
final_results.sort(key=lambda x: (x['REVENUE'], x['C_CUSTKEY'], x['C_NAME'], -x['C_ACCTBAL']))

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=list(final_results[0].keys()))
    writer.writeheader()
    for data in final_results:
        writer.writerow(data)

# Close MongoDB connection
mongo_client.close()
