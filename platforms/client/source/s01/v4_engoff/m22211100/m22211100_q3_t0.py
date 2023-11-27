import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    db='tpch',
    user='root',
    passwd='my-secret-pw',
    host='mysql',
)
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
client = pymongo.MongoClient('mongodb', 27017)
mongodb = client['tpch']
customer_col = mongodb['customer']

# Retrieving BUILDING market segment's customer keys
building_customers = customer_col.find({"C_MKTSEGMENT": "BUILDING"}, {"C_CUSTKEY": 1, "_id": 0})
building_custkeys = [cust['C_CUSTKEY'] for cust in building_customers]

# SQL Query
sql_query = """
SELECT 
    o.O_ORDERKEY, 
    o.O_SHIPPRIORITY, 
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
FROM 
    orders o 
JOIN 
    lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE 
    o.O_ORDERDATE < '1995-03-15' 
    AND o.O_CUSTKEY IN (%s)
    AND l.L_SHIPDATE > '1995-03-15'
GROUP BY 
    o.O_ORDERKEY
ORDER BY 
    revenue DESC, 
    o.O_ORDERKEY;
""" % ','.join(map(str, building_custkeys))  # Constructing the IN clause

mysql_cursor.execute(sql_query)
result = mysql_cursor.fetchall()

# Writing to CSV
with open('query_output.csv', mode='w') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(['O_ORDERKEY', 'O_SHIPPRIORITY', 'REVENUE'])
    for row in result:
        csv_writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
client.close()
