import pymysql
import pymongo
import csv

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_col = mongo_db['part']

# Find parts with the specific brand and container type from MongoDB
part_filter = {'P_BRAND': 'Brand#23', 'P_CONTAINER': 'MED BAG'}
parts = part_col.find(part_filter, {'P_PARTKEY': 1})

# Get all relevant part keys
part_keys = [p['P_PARTKEY'] for p in parts]

# Construct the custom query to be executed on the MySQL database
sql_query = """
    SELECT L_ORDERKEY, SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY_EXTENDED_PRICE
    FROM lineitem 
    WHERE L_PARTKEY IN (%s) 
    AND L_QUANTITY < (
        SELECT 0.2 * AVG(L_QUANTITY) FROM lineitem WHERE L_PARTKEY = lineitem.L_PARTKEY
    )
    GROUP BY L_ORDERKEY;
""" % ','.join(map(str, part_keys))

# Execute the MySQL query
mysql_cursor.execute(sql_query)

# Fetch all the results
results = mysql_cursor.fetchall()

# Write results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Write the header
    csvwriter.writerow(['L_ORDERKEY', 'AVG_YEARLY_EXTENDEDPRICE'])
    # Write the data
    csvwriter.writerows(results)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
