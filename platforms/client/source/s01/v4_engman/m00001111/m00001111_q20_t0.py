import pymysql
import pymongo
import csv
from datetime import datetime

# Function to retrieve suppliers with an excess of forest parts from MySQL
def get_mysql_supplier_forest_parts():
    try:
        connection = pymysql.connect(host='mysql',
                                     user='root',
                                     password='my-secret-pw',
                                     database='tpch')
        with connection.cursor() as cursor:
            query = """
            SELECT s.S_SUPPKEY, s.S_NAME
            FROM supplier AS s
            JOIN nation AS n ON s.S_NATIONKEY = n.N_NATIONKEY
            JOIN partsupp AS ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
            JOIN part AS p ON ps.PS_PARTKEY = p.P_PARTKEY
            WHERE n.N_NAME = 'CANADA'
            AND p.P_NAME LIKE '%forest%'
            GROUP BY s.S_SUPPKEY, s.S_NAME
            HAVING SUM(ps.PS_AVAILQTY) > 0.5 * (
                SELECT SUM(l.L_QUANTITY)
                FROM lineitem AS l
                WHERE l.L_PARTKEY = ps.PS_PARTKEY
                AND l.L_SUPPKEY = s.S_SUPPKEY
                AND l.L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
            );
            """
            cursor.execute(query)
            return cursor.fetchall()
    except Exception as e:
        print("Error while querying MySQL:", e)
    finally:
        if connection:
            connection.close()

# Function to get the partsupp collection from MongoDB toemulate a JOIN
def get_mongodb_partsupp_collection(mongo_collection):
    result = mongo_collection.aggregate([
        {
            "$lookup": {
                "from": "lineitem",
                "localField": "PS_SUPPKEY",
                "foreignField": "L_SUPPKEY",
                "as": "lineitem_docs"
            }
        },
        {
            "$match": {
                "lineitem_docs.L_SHIPDATE": {
                    "$gte": datetime(1994, 1, 1),
                    "$lt": datetime(1995, 1, 1)
                },
                "lineitem_docs.L_PARTKEY": "$PS_PARTKEY"
            }
        }
    ])
    return list(result)

# Function to combine data from MySQL and MongoDB
def generate_query_output(suppliers, partsupps):
    fieldnames = ['S_SUPPKEY', 'S_NAME', 'EXCESS_QUANTITY']
    with open('query_output.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for supplier in suppliers:
            excess_qty = 0
            for partsupp in partsupps:
                if partsupp['PS_SUPPKEY'] == supplier[0]:
                    for lineitem in partsupp['lineitem_docs']:
                        excess_qty += lineitem['L_QUANTITY']

            if excess_qty > 0:
                writer.writerow({
                    'S_SUPPKEY': supplier[0],
                    'S_NAME': supplier[1],
                    'EXCESS_QUANTITY': excess_qty
                })

# Main execution
if __name__ == '__main__':
    # Retrieve forest part suppliers from MySQL
    suppliers_with_forest_parts = get_mysql_supplier_forest_parts()
    
    # Connecting to MongoDB
    try:
        mongo_client = pymongo.MongoClient('mongodb', 27017)
        mongodb = mongo_client['tpch']
        partsupp_collection = mongodb['partsupp']
        
        # Emulate JOINs to retrieve partsupp information
        partsupp_data = get_mongodb_partsupp_collection(partsupp_collection)

    except Exception as e:
        print("Error while querying MongoDB:", e)
    finally:
        mongo_client.close()

    # Generate the output CSV file
    generate_query_output(suppliers_with_forest_parts, partsupp_data)
