import pymysql
import pymongo
import csv

# Connect to MySQL
conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query MySQL for parts information
with conn.cursor() as cursor:
    cursor.execute("""
    SELECT P_PARTKEY FROM part
    WHERE
        (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5)
     OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10)
     OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
    """)
    qualifying_parts = cursor.fetchall()

# Process qualifying parts to filter lineitem records from MongoDB
qualifying_part_keys = [part[0] for part in qualifying_parts]

# Define function to qualify lineitem document
def qualifies(doc):
    return (
        doc['L_PARTKEY'] in qualifying_part_keys
        and doc['L_SHIPMODE'] in ['AIR', 'AIR REG']
        and doc['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'
        and (
            (doc['L_QUANTITY'] >= 1 and doc['L_QUANTITY'] <= 11 and doc['L_PARTKEY'] in qualifying_part_keys)
            or (doc['L_QUANTITY'] >= 10 and doc['L_QUANTITY'] <= 20 and doc['L_PARTKEY'] in qualifying_part_keys)
            or (doc['L_QUANTITY'] >= 20 and doc['L_QUANTITY'] <= 30 and doc['L_PARTKEY'] in qualifying_part_keys)
        )
    )

# Apply the filter to the lineitem collection to compute the revenue
revenue = 0.0
lineitem_collection = mongo_db['lineitem']
for doc in lineitem_collection.find():
    if qualifies(doc):
        revenue += doc['L_EXTENDEDPRICE'] * (1 - doc['L_DISCOUNT'])

# Write the result to a file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['REVENUE'])
    csv_writer.writerow([revenue])

# Close the connections
conn.close()
mongo_client.close()
