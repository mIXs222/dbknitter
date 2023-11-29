import pymysql
import pymongo
import csv
import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn.cursor() as cursor:
    sql = """
    SELECT
        l_shipmode,
        SUM(CASE WHEN o_orderpriority IN ('URGENT', 'HIGH') THEN 1 ELSE 0 END) AS high_priority_count,
        SUM(CASE WHEN o_orderpriority NOT IN ('URGENT', 'HIGH') THEN 1 ELSE 0 END) AS low_priority_count
    FROM lineitem
    INNER JOIN orders ON l_orderkey = o_orderkey
    WHERE
        l_shipmode IN ('MAIL', 'SHIP')
        AND l_receiptdate BETWEEN '1994-01-01' AND '1995-01-01'
        AND l_receiptdate > l_commitdate
        AND l_shipdate < l_commitdate
    GROUP BY l_shipmode
    ORDER BY l_shipmode
    """
    cursor.execute(sql)
    mysql_results = cursor.fetchall()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client["tpch"]
orders_collection = mongodb_db["orders"]
mongo_query = {
    "O_ORDERDATE": {
        "$gte": datetime.datetime(1994, 1, 1),
        "$lt": datetime.datetime(1995, 1, 1)
    }
}
mongo_orders = orders_collection.find(mongo_query)

# We assume that the lineitem data has been validated and matches the orders in MongoDB
# Convert MongoDB results to match the MySQL output format for merging
mongodb_results = []
for order in mongo_orders:
    ship_mode = order.get("O_SHIPMODE")
    if ship_mode in ["MAIL", "SHIP"]:
        high_priority_count = 1 if order["O_ORDERPRIORITY"] in ("URGENT", "HIGH") else 0
        low_priority_count = 1 if order["O_ORDERPRIORITY"] not in ("URGENT", "HIGH") else 0
        mongodb_results.append((ship_mode, high_priority_count, low_priority_count))

# Merge results from MySQL and MongoDB
combined_results = {}
for ship_mode, high_count, low_count in mysql_results + mongodb_results:
    if ship_mode not in combined_results:
        combined_results[ship_mode] = {"high_priority_count": 0, "low_priority_count": 0}
    combined_results[ship_mode]["high_priority_count"] += high_count
    combined_results[ship_mode]["low_priority_count"] += low_count

# Write the combined results to query_output.csv
with open("query_output.csv", "w") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["l_shipmode", "high_priority_count", "low_priority_count"])
    for ship_mode, counts in sorted(combined_results.items()):
        csvwriter.writerow([ship_mode, counts["high_priority_count"], counts["low_priority_count"]])

# Close the database connections
mysql_conn.close()
mongodb_client.close()
