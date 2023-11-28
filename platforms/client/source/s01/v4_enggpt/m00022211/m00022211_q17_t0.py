import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Fetch parts data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute(
        "SELECT P_PARTKEY, P_BRAND, P_CONTAINER "
        "FROM part "
        "WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'"
    )
    parts_data = cursor.fetchall()

part_keys = [p[0] for p in parts_data]

# Prepare MongoDB query
match_stage = {
    '$match': {
        'L_PARTKEY': {'$in': part_keys}
    }
}
group_stage = {
    '$group': {
        '_id': '$L_PARTKEY',
        'average_quantity': {'$avg': '$L_QUANTITY'},
        'valid_lineitems': {
            '$push': {
                'L_EXTENDEDPRICE': '$L_EXTENDEDPRICE',
                'L_QUANTITY': '$L_QUANTITY'
            }
        }
    }
}

# Fetch lineitem data from MongoDB and filter in memory
lineitems = list(mongodb.lineitem.aggregate([match_stage, group_stage]))

# Prepare CSV output
output_rows = [['average_yearly_extended_price']]

for item in lineitems:
    valid_extended_prices = [
        li['L_EXTENDEDPRICE']
        for li in item['valid_lineitems']
        if li['L_QUANTITY'] < 0.2 * item['average_quantity']
    ]

    if valid_extended_prices:
        average_yearly_extended_price = sum(valid_extended_prices) / 7.0
        output_rows.append([average_yearly_extended_price])

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(output_rows)

# Close the connections
mysql_conn.close()
mongo_client.close()
