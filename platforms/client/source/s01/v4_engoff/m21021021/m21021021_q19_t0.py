import pymysql
import pymongo
import csv

def get_mysql_connection():
    return pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

def get_mongo_connection():
    client = pymongo.MongoClient('mongodb', 27017)
    return client['tpch']

def execute():
    mysql_conn = get_mysql_connection()
    mongo_db = get_mongo_connection()

    desired_containers = [
        ("'SM CASE'", "'SM BOX'", "'SM PACK'", "'SM PKG'", 1, 5, 1, 11, '12'),
        ("'MED BAG'", "'MED BOX'", "'MED PKG'", "'MED PACK'", 1, 10, 10, 20, '23'),
        ("'LG CASE'", "'LG BOX'", "'LG PACK'", "'LG PKG'", 1, 15, 20, 30, '34')
    ]
    
    results = []

    with mysql_conn.cursor() as cursor:
        for containers in desired_containers:
            sql = """
            SELECT P_PARTKEY FROM part
            WHERE P_BRAND = %s AND P_CONTAINER IN (%s, %s, %s, %s) AND
            P_SIZE BETWEEN %s AND %s
            """ % containers[:7]

            cursor.execute(sql)
            part_keys = [row[0] for row in cursor.fetchall()]

            mongo_lineitems = mongo_db['lineitem'].find({
                'L_PARTKEY': {'$in': part_keys},
                'L_QUANTITY': {'$gte': containers[7], '$lte': containers[8]},
                'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}
            })

            for lineitem in mongo_lineitems:
                revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
                results.append({
                    'L_ORDERKEY': lineitem['L_ORDERKEY'],
                    'REVENUE': revenue
                })

    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['L_ORDERKEY', 'REVENUE']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(results)
    
    mysql_conn.close()

if __name__ == "__main__":
    execute()
