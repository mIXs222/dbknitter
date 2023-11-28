import pymysql
import pymongo
import csv

# Establish a connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Establish a connection to MongoDB
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']

# Prepare SQL query for MySQL data
mysql_query = """
SELECT
    L_ORDERKEY,
    L_PARTKEY,
    L_QUANTITY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_SHIPMODE,
    L_SHIPINSTRUCT
FROM
    lineitem
WHERE
    L_SHIPMODE IN ('AIR', 'AIR REG') AND
    L_SHIPINSTRUCT = 'DELIVER IN PERSON'
"""

with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['PARTKEY', 'ORDERKEY', 'REVENUE'])

    # Use a cursor to perform the query
    with mysql_conn.cursor() as cursor:
        cursor.execute(mysql_query)
        lineitems = cursor.fetchall()

        # Query MongoDB for 'part' data
        for item in lineitems:
            part = mongodb_db.part.find_one({
                'P_PARTKEY': item[1],
                'P_BRAND': {'$in': ['Brand#12', 'Brand#23', 'Brand#34']},
                'P_CONTAINER': {'$in': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']},
                'P_SIZE': {'$gte': 1, '$lte': 15}
            })
            if part:
                brand = part['P_BRAND']
                container = part['P_CONTAINER']
                size = part['P_SIZE']
                quantity = item[2]

                # Check conditions based on the brand
                if (brand == 'Brand#12' and 1 <= quantity <= 11 and 1 <= size <= 5) or \
                   (brand == 'Brand#23' and 10 <= quantity <= 20 and 1 <= size <= 10) or \
                   (brand == 'Brand#34' and 20 <= quantity <= 30 and 1 <= size <= 15):
                    # Calculate revenue
                    revenue = item[3] * (1 - item[4])  # extended price * (1 - discount)
                    writer.writerow([item[1], item[0], revenue])

mysql_conn.close()
mongodb_client.close()
