import pymysql
import pymongo
import csv

mysql_conn_info = {
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
    'database': 'tpch'
}

mongo_conn_info = {
    'host': 'mongodb',
    'port': 27017,
    'database': 'tpch'
}

def get_mysql_connection(mysql_info):
    return pymysql.connect(
        host=mysql_info['host'],
        user=mysql_info['user'],
        password=mysql_info['password'],
        database=mysql_info['database']
    )

def get_mongo_connection(mongo_info):
    client = pymongo.MongoClient(host=mongo_info['host'], port=mongo_info['port'])
    return client[mongo_info['database']]

def fetch_from_mysql(conn):
    with conn.cursor() as cursor:
        sql_query = """
        SELECT P_PARTKEY, P_BRAND, P_TYPE, P_SIZE
        FROM part
        WHERE P_BRAND != 'Brand#45'
        AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
        AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
        """
        cursor.execute(sql_query)
        return cursor.fetchall()

def fetch_from_mongodb(db):
    supplier_parts = db.partsupp.aggregate([
        {
            "$lookup": {
                "from": "supplier",
                "localField": "PS_SUPPKEY",
                "foreignField": "S_SUPPKEY",
                "as": "supplier"
            }
        },
        {
            "$match": {
                "supplier.S_COMMENT": { "$not": { "$regex": "Customer Complaints" } }
            }
        },
        {
            "$project": {
                "PS_PARTKEY": 1,
                "PS_SUPPKEY": 1,
                "_id": 0
            }
        }
    ])
    
    # Convert to a set for fast lookup
    supplier_parts_data = {(part['PS_PARTKEY'], part['PS_SUPPKEY']) for part in supplier_parts}
    return supplier_parts_data

def main():
    # Connect to MySQL and fetch parts data
    mysql_conn = get_mysql_connection(mysql_conn_info)
    mysql_parts_data = fetch_from_mysql(mysql_conn)
    mysql_conn.close()

    # Connect to MongoDB and fetch supplier data
    mongo_conn = get_mongo_connection(mongo_conn_info)
    supplier_parts_data = fetch_from_mongodb(mongo_conn)

    # Prepare the aggregation output structure
    aggregated_data = {}

    for part in mysql_parts_data:
        partkey = part[0]
        brand = part[1]
        ptype = part[2]
        size = part[3]

        # Check for suppliers related to this part
        suppliers_count = sum(1 for suppid in range(1, 10000) if (partkey, suppid) in supplier_parts_data)
        
        # Skip if no suppliers
        if not suppliers_count:
            continue

        group_key = (brand, ptype, size)
        if group_key not in aggregated_data:
            aggregated_data[group_key] = {'SUPPLIER_CNT': 0}

        aggregated_data[group_key]['SUPPLIER_CNT'] += suppliers_count

    # Sort the data as required by the problem statement
    sorted_data = sorted(aggregated_data.items(), 
                         key=lambda x: (-x[1]['SUPPLIER_CNT'], x[0][0], x[0][1], x[0][2]))

    # Write to CSV
    with open('query_output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])
        for item in sorted_data:
            writer.writerow([item[0][0], item[0][1], item[0][2], item[1]['SUPPLIER_CNT']])

if __name__ == '__main__':
    main()
