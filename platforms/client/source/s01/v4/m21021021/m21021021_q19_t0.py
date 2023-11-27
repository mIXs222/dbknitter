import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_lineitem = mongo_db.lineitem

# Fetch part data from MySQL
part_query = '''
SELECT P_PARTKEY, P_BRAND, P_SIZE, P_CONTAINER FROM part
WHERE (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5)
   OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10)
   OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
'''
mysql_cursor.execute(part_query)
part_data = mysql_cursor.fetchall()

# Create a partkey lookup dictionary from part data
partkey_data = {}
for row in part_data:
    partkey, brand, size, container = row
    partkey_data[partkey] = {'brand': brand, 'size': size, 'container': container}

mysql_conn.close()

# Aggregate revenue from lineitem in MongoDB
revenue = 0
query_filter = {
    '$or': [
        {'L_QUANTITY': {'$gte': 1, '$lte': 11}, 'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}, 'L_SHIPINSTRUCT': 'DELIVER IN PERSON'},
        {'L_QUANTITY': {'$gte': 10, '$lte': 20}, 'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}, 'L_SHIPINSTRUCT': 'DELIVER IN PERSON'},
        {'L_QUANTITY': {'$gte': 20, '$lte': 30}, 'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}, 'L_SHIPINSTRUCT': 'DELIVER IN PERSON'},
    ]
}

for lineitem in mongo_lineitem.find(query_filter):
    partkey = lineitem['L_PARTKEY']
    if partkey in partkey_data:
        brands_conditions = ['Brand#12', 'Brand#23', 'Brand#34']
        if partkey_data[partkey]['brand'] not in brands_conditions:
            continue
  
        size_conditions = list(range(1, 16))  # 1 to 15 inclusive
        if partkey_data[partkey]['size'] not in size_conditions:
            continue

        quantity = lineitem['L_QUANTITY']
        if not ((1 <= quantity <= 11) or (10 <= quantity <= 20) or (20 <= quantity <= 30)):
            continue

        extended_price = lineitem['L_EXTENDEDPRICE']
        discount = lineitem['L_DISCOUNT']
        revenue += extended_price * (1 - discount)

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['REVENUE'])
    writer.writerow([revenue])

print("Query output successfully written to query_output.csv")
