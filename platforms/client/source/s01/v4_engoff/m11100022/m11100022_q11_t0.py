import pymysql
import pymongo
import csv

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
nation_collection = mongo_db['nation']

# Get nation key for 'GERMANY'
germany_nation_key = None
for nation in nation_collection.find({"N_NAME": "GERMANY"}):
    germany_nation_key = nation['N_NATIONKEY']
    break

# Check if Germany nation key was found
if germany_nation_key is not None:
    # Query to select important stock values in MySQL
    mysql_query = """
    SELECT PS_PARTKEY, SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS TOTAL_VALUE
    FROM partsupp
    JOIN supplier ON supplier.S_SUPPKEY = partsupp.PS_SUPPKEY
    WHERE supplier.S_NATIONKEY = %s
    GROUP BY PS_PARTKEY
    HAVING SUM(PS_SUPPLYCOST * PS_AVAILQTY) > 0.0001 * (SELECT SUM(PS_SUPPLYCOST * PS_AVAILQTY) FROM partsupp)
    ORDER BY TOTAL_VALUE DESC
    """
    mysql_cursor.execute(mysql_query, (germany_nation_key,))

    # Write query output to CSV file
    with open('query_output.csv', mode='w', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(["PARTKEY", "VALUE"])  # Header
        for row in mysql_cursor.fetchall():
            csv_writer.writerow(row)
else:
    print("Nation GERMANY not found in MongoDB")

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
