import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

try:
    # Fetch nation key for GERMANY from MongoDB nation table
    germany_nation = mongo_db.nation.find_one({'N_NAME': 'GERMANY'})
    germany_nationkey = germany_nation['N_NATIONKEY']

    # Fetch supplier keys for suppliers in GERMANY from MySQL supplier table
    mysql_cursor = mysql_conn.cursor()
    supplier_query = "SELECT S_SUPPKEY FROM supplier WHERE S_NATIONKEY = %s"
    mysql_cursor.execute(supplier_query, (germany_nationkey,))
    supplier_keys = [row[0] for row in mysql_cursor.fetchall()]
    mysql_cursor.close()

    # Fetch partsupp data from MongoDB partsupp table for suppliers in GERMANY
    part_supps = mongo_db.partsupp.find({'PS_SUPPKEY': {'$in': supplier_keys}})

    # Calculate total value for each part and select those with significant stock value
    part_values = {}
    for ps in part_supps:
        value = ps['PS_AVAILQTY'] * ps['PS_SUPPLYCOST']
        if ps['PS_PARTKEY'] in part_values:
            part_values[ps['PS_PARTKEY']] += value
        else:
            part_values[ps['PS_PARTKEY']] = value

    # Filters parts of significant stock value
    significant_parts = [(k, v) for k, v in part_values.items() if v / sum(part_values.values()) > 0.0001]

    # Sort parts by value in descending order
    significant_parts_sorted = sorted(significant_parts, key=lambda part: part[1], reverse=True)

    # Write query results to CSV
    with open('query_output.csv', mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['PS_PARTKEY', 'STOCK_VALUE'])
        for part in significant_parts_sorted:
            writer.writerow(part)

finally:
    # Close all connections
    mysql_conn.close()
    mongo_client.close()
