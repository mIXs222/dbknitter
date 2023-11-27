# python code stored as combined_query.py
import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch',
                                   cursorclass=pymysql.cursors.Cursor)  # Use default cursor, not DictCursor

try:
    with mysql_connection.cursor() as cursor:
        # Query to get lineitem data
        mysql_query = """
        SELECT
            L_ORDERKEY, L_SHIPMODE
        FROM
            lineitem
        WHERE
            L_SHIPMODE IN ('MAIL', 'SHIP')
            AND L_COMMITDATE < L_RECEIPTDATE
            AND L_SHIPDATE < L_COMMITDATE
            AND L_RECEIPTDATE >= '1994-01-01'
            AND L_RECEIPTDATE < '1995-01-01'
        """
        cursor.execute(mysql_query)
        lineitem_data = cursor.fetchall()

finally:
    mysql_connection.close()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client["tpch"]

# Query to get orders data
mongodb_query = {
    "O_ORDERDATE": {
        "$gte": "1994-01-01",
        "$lt": "1995-01-01"
    }
}
orders_data = list(mongodb_db.orders.find(mongodb_query))

# Process and filter orders data into a dictionary keyed by O_ORDERKEY
orders_dict = {order['O_ORDERKEY']: order for order in orders_data}

# Prepare the final result data structure
result = {}

# Combine the data from MySQL and MongoDB
for lineitem in lineitem_data:
    orderkey = lineitem[0]
    shipmode = lineitem[1]

    if orderkey in orders_dict:
        order = orders_dict[orderkey]
        priority = order["O_ORDERPRIORITY"]

        # Create a new entry in the result dictionary if necessary
        if shipmode not in result:
            result[shipmode] = [0, 0]

        # Update high and low line counts
        if priority in ['1-URGENT', '2-HIGH']:
            result[shipmode][0] += 1
        else:
            result[shipmode][1] += 1

# Write output to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
    
    for shipmode, counts in sorted(result.items()):
        csvwriter.writerow([shipmode] + counts)
