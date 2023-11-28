# import required modules
import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch')

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client.tpch

try:
    # Execute query for MySQL to get supplier information for suppliers in Germany
    with mysql_conn.cursor() as mysql_cursor:
        mysql_cursor.execute("""
            SELECT s.S_SUPPKEY
            FROM supplier s
            JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
            WHERE n.N_NAME = 'GERMANY'
        """)
        german_suppliers = {row[0] for row in mysql_cursor.fetchall()}
    
    # Query MongoDB for partsupps that match the German suppliers
    partsupps_cursor = mongo_db.partsupp.find(
        {'PS_SUPPKEY': {'$in': list(german_suppliers)}}
    )
    
    # Calculate total values and filter results
    parts_values = {}
    for partsupp in partsupps_cursor:
        key = partsupp['PS_PARTKEY']
        value = partsupp['PS_SUPPLYCOST'] * partsupp['PS_AVAILQTY']
        if key in parts_values:
            parts_values[key] += value
        else:
            parts_values[key] = value

    # Filter part keys based on the threshold and prepare results
    threshold = 0.05  # replace with actual value if available
    total_value = sum(parts_values.values())
    filtered_parts_values = {
        key: value for key, value in parts_values.items() if value > threshold * total_value
    }

    # Sort parts by value and write to CSV
    sorted_parts_values = sorted(filtered_parts_values.items(), key=lambda item: item[1], reverse=True)
    
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['PARTKEY', 'VALUE'])
        for partkey, value in sorted_parts_values:
            csvwriter.writerow([partkey, value])

finally:
    # Close connections
    mysql_conn.close()
    mongo_client.close()
