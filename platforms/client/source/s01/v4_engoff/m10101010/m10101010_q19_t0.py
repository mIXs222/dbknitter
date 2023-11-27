# Python code in 'execute_query.py'
import pymysql
import pymongo
import csv

# Function to fetch data from MySQL
def fetch_mysql_data():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch')
    
    try:
        with connection.cursor() as cursor:
            sql = """
            SELECT L_EXTENDEDPRICE, L_DISCOUNT, L_QUANTITY, L_SHIPMODE, P_SIZE, P_BRAND, P_CONTAINER
            FROM lineitem LEFT JOIN part ON L_PARTKEY = P_PARTKEY
            WHERE (
                   (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND L_QUANTITY >= 1 AND L_QUANTITY <= 11 AND P_SIZE BETWEEN 1 AND 5)
                OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND L_QUANTITY >= 10 AND L_QUANTITY <= 20 AND P_SIZE BETWEEN 1 AND 10)
                OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND L_QUANTITY >= 20 AND L_QUANTITY <= 30 AND P_SIZE BETWEEN 1 AND 15)
            ) AND (L_SHIPMODE = 'AIR' OR L_SHIPMODE = 'AIR REG')
            """
            cursor.execute(sql)
            result = cursor.fetchall()
    finally:
        connection.close()

    return result

# Function to fetch data from MongoDB
def fetch_mongodb_data():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client.tpch
    cursor = db.part.find({
        "$or": [
            {"P_BRAND": "Brand#12", "P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}, "P_SIZE": {"$gte": 1, "$lte": 5}},
            {"P_BRAND": "Brand#23", "P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]}, "P_SIZE": {"$gte": 1, "$lte": 10}},
            {"P_BRAND": "Brand#34", "P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]}, "P_SIZE": {"$gte": 1, "$lte": 15}},
        ]
    })
    result = {doc["P_PARTKEY"]: doc for doc in cursor}
    client.close()

    return result

# Main logic to combine the data and perform calculations
def main():
    mysql_data = fetch_mysql_data()
    mongodb_data = fetch_mongodb_data()

    combined_data = []

    for row in mysql_data:
        l_extendedprice, l_discount, l_quantity, l_shipmode, p_size, p_brand, p_container = row
        if int(p_brand[-2:]) == 12 and l_quantity >= 1 and l_quantity <= 11 and 1 <= p_size <= 5:
            discounted_price = l_extendedprice * (1 - l_discount)
            combined_data.append(discounted_price)
        elif int(p_brand[-2:]) == 23 and l_quantity >= 10 and l_quantity <= 20 and 1 <= p_size <= 10:
            discounted_price = l_extendedprice * (1 - l_discount)
            combined_data.append(discounted_price)
        elif int(p_brand[-2:]) == 34 and l_quantity >= 20 and l_quantity <= 30 and 1 <= p_size <= 15:
            discounted_price = l_extendedprice * (1 - l_discount)
            combined_data.append(discounted_price)

    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['DiscountedPrice'])
        for val in combined_data:
            csvwriter.writerow([val])

if __name__ == '__main__':
    main()
