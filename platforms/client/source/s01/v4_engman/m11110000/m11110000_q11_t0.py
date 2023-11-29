import pymysql
import pymongo
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch'
)

# MongoDB connection setup
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client['tpch']

# Querying MySQL to get partsupp details
with mysql_conn.cursor() as mysql_cursor:
    mysql_query = """
    SELECT PS_PARTKEY, SUM(PS_AVAILQTY * PS_SUPPLYCOST) AS value
    FROM partsupp
    WHERE PS_SUPPKEY IN (
        SELECT S_SUPPKEY 
        FROM supplier 
        WHERE S_NATIONKEY = (
            SELECT N_NATIONKEY 
            FROM nation 
            WHERE N_NAME = 'GERMANY'
        )
    )
    GROUP BY PS_PARTKEY
    HAVING value > 0.0001
    ORDER BY value DESC;
    """
    mysql_cursor.execute(mysql_query)

    # Store MySQL results
    mysql_results = {row[0]: row[1] for row in mysql_cursor}

# Querying MongoDB to get supplier and nation information
nation_filter = {"N_NAME": "GERMANY"}
nation_data = mongodb_db.nation.find_one(nation_filter)

supplier_filter = {"S_NATIONKEY": nation_data['N_NATIONKEY']} if nation_data else {}
supplier_data = mongodb_db.supplier.find(supplier_filter)

# Get supplier keys in Germany
supplier_keys = [supplier['S_SUPPKEY'] for supplier in supplier_data]

# Final results initialization
final_results = [
    {'PS_PARTKEY': partkey, 'value': value}
    for partkey, value in mysql_results.items()
    if partkey in supplier_keys
]

# Output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['PS_PARTKEY', 'value']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for data in final_results:
        writer.writerow(data)

# Close connections
mysql_conn.close()
mongodb_client.close()
