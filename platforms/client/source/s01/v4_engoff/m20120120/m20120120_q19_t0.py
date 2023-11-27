import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
part_collection = mongodb_db['part']

# Query for parts data from MongoDB
brand_containers_map = {
    "12": ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'],
    "23": ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'],
    "34": ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'],
}
size_ranges_map = {
    "12": (1, 5),
    "23": (1, 10),
    "34": (1, 15),
}
quantity_ranges_map = {
    "12": (1, 11),
    "23": (10, 20),
    "34": (20, 30),
}

# Storing the query results
results = []

# Processing each type of part
for brand_id, containers in brand_containers_map.items():
    size_range = size_ranges_map[brand_id]
    quantity_range = quantity_ranges_map[brand_id]

    # Getting part keys from MongoDB
    part_keys = part_collection.find(
        {
            'P_BRAND': brand_id,
            'P_CONTAINER': {'$in': containers},
            'P_SIZE': {'$gte': size_range[0], '$lte': size_range[1]}
        },
        {'P_PARTKEY': 1}
    )
    p_partkeys = [part['P_PARTKEY'] for part in part_keys]

    # Getting lineitem data from MySQL
    sql_query = """
    SELECT L_ORDERKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue
    FROM lineitem
    WHERE L_PARTKEY IN (%s)
    AND L_QUANTITY >= %s AND L_QUANTITY <= %s
    AND L_SHIPMODE IN ('AIR', 'AIR REG')
    AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    GROUP BY L_ORDERKEY
    """
    mysql_cursor.execute(sql_query, [p_partkeys, quantity_range[0], quantity_range[1]])
    results.extend(mysql_cursor.fetchall())

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['L_ORDERKEY', 'REVENUE'])
    writer.writerows(results)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
