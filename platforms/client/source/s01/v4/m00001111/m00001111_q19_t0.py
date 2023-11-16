import pymysql
import pymongo
import csv

def fetch_mysql_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT P_PARTKEY, P_BRAND, P_CONTAINER, P_SIZE
                FROM part
                WHERE (
                    P_BRAND = 'Brand#12'
                    AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    AND P_SIZE BETWEEN 1 AND 5
                )
                OR (
                    P_BRAND = 'Brand#23'
                    AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                    AND P_SIZE BETWEEN 1 AND 10
                )
                OR (
                    P_BRAND = 'Brand#34'
                    AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                    AND P_SIZE BETWEEN 1 AND 15
                )
                """)
            part_data = cursor.fetchall()
    finally:
        connection.close()
    return part_data

def fetch_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client.tpch
    lineitem_collection = db.lineitem
    line_items_cursor = lineitem_collection.find({
        "$or": [
            {"L_QUANTITY": {"$gte": 1, "$lte": 11}, "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]}, "L_SHIPINSTRUCT": "DELIVER IN PERSON"},
            {"L_QUANTITY": {"$gte": 10, "$lte": 20}, "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]}, "L_SHIPINSTRUCT": "DELIVER IN PERSON"},
            {"L_QUANTITY": {"$gte": 20, "$lte": 30}, "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]}, "L_SHIPINSTRUCT": "DELIVER IN PERSON"}
        ]
    })
    return list(line_items_cursor)

def main():
    part_data = fetch_mysql_data()
    lineitem_data = fetch_mongodb_data()
    
    # Convert part_data to dict for easier lookup
    part_dict = {(row[0], row[1], row[2]): row[3] for row in part_data}
    
    # Calculate the revenue
    revenue = 0
    for lineitem in lineitem_data:
        partkey = lineitem["L_PARTKEY"]
        quantity = lineitem["L_QUANTITY"]
        eprice = lineitem["L_EXTENDEDPRICE"]
        discount = lineitem["L_DISCOUNT"]
        brand = lineitem.get("L_BRAND")
        container = lineitem.get("L_CONTAINER")
        
        size = part_dict.get((partkey, brand, container))
        if size is not None:
            revenue += eprice * (1 - discount)

    # Output to the file
    with open('query_output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['REVENUE'])
        writer.writerow([revenue])
        
if __name__ == "__main__":
    main()
