import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# MySQL query
mysql_query = """
SELECT
    O_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE
FROM
    orders
WHERE
    O_ORDERDATE >= '1993-10-01'
    AND O_ORDERDATE < '1994-01-01'
"""

# Execute MySQL query
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(mysql_query)

# Fetch data from MySQL
order_details = {(row[0], row[1]): row[2] for row in mysql_cursor}
mysql_cursor.close()

# MongoDB query
mongodb_query = [
    {
        '$match': {
            'L_RETURNFLAG': 'R',
            'L_ORDERKEY': {'$in': list(order_details.keys())}
        }
    },
    {
        '$group': {
            '_id': '$L_ORDERKEY',
            'REVENUE': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]}}
        }
    }
]

# Execute MongoDB query for lineitem
lineitem_results = {doc['_id']: doc['REVENUE'] for doc in mongodb_db['lineitem'].aggregate(mongodb_query)}

# Join and complete the query to get `customer` and `nation`
final_query = """
SELECT
    C_CUSTKEY,
    C_NAME,
    C_ACCTBAL,
    N_NAME,
    C_ADDRESS,
    C_PHONE,
    C_COMMENT
FROM
    customer,
    nation
WHERE
    C_NATIONKEY = N_NATIONKEY
    AND C_CUSTKEY IN (%s)
"""
custkeys = ','.join(map(str, order_details.keys()))
final_cursor = mysql_conn.cursor()
final_cursor.execute(final_query, [custkeys])

# Write output to csv
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'])
    for c_custkey, c_name, c_acctbal, n_name, c_address, c_phone, c_comment in final_cursor:
        for (custkey, orderkey), orderdate in order_details.items():
            if custkey == c_custkey:
                writer.writerow([c_custkey, c_name, lineitem_results.get((custkey, orderkey), 0), c_acctbal, n_name, c_address, c_phone, c_comment])

# Close database connections
final_cursor.close()
mysql_conn.close()
mongodb_client.close()
