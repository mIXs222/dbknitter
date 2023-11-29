import pymysql
import pymongo
import csv

# Define MongoDB connection and fetch parts with specified attributes
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
parts_collection = mongodb['part']

condition1 = {
    '$and': [
        { 'P_BRAND': 'Brand#12' },
        { 'P_CONTAINER': { '$in': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'] }},
        { 'P_SIZE': { '$gte': 1, '$lte': 5 }}
    ]
}
condition2 = {
    '$and': [
        { 'P_BRAND': 'Brand#23' },
        { 'P_CONTAINER': { '$in': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'] }},
        { 'P_SIZE': { '$gte': 1, '$lte': 10 }}
    ]
}
condition3 = {
    '$and': [
        { 'P_BRAND': 'Brand#34' },
        { 'P_CONTAINER': { '$in': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'] }},
        { 'P_SIZE': { '$gte': 1, '$lte': 15 }}
    ]
}
part_keys = {
    condition['P_PARTKEY']
    for condition in (condition1, condition2, condition3)
    for part in parts_collection.find(condition, {'_id': 0, 'P_PARTKEY': 1})
}

# Define MySQL connection
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch',
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.Cursor)

try:
    with mysql_connection.cursor() as cursor:
        # Query to calculate the revenue
        sql_query = """
        SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
        FROM lineitem
        WHERE
            L_PARTKEY IN %s AND
            L_SHIPMODE IN ('AIR', 'AIR REG') AND
            L_SHIPINSTRUCT = 'DELIVER IN PERSON' AND
            ((L_QUANTITY >= 1 AND L_QUANTITY <= 11) OR
            (L_QUANTITY >= 10 AND L_QUANTITY <= 20) OR
            (L_QUANTITY >= 20 AND L_QUANTITY <= 30))
        """
        cursor.execute(sql_query, (list(part_keys),))
        revenue = cursor.fetchone()
        
        # Write the result to a CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(['REVENUE'])
            csv_writer.writerow(revenue)
            
finally:
    mysql_connection.close()
