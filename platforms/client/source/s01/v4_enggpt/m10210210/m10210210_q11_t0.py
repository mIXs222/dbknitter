import pymysql
import pymongo
import csv

# Define the threshold value
threshold_percentage = 0.05  # Placeholder, adjust as needed

# Connect to mysql
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to mongodb
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
nation_collection = mongo_db['nation']
supplier_collection = mongo_db['supplier']

# Find the Germany nationkey from MongoDB
german_nation = nation_collection.find_one({'N_NAME': 'GERMANY'})
if not german_nation:
    raise ValueError("No nation found with the name GERMANY")

german_nation_key = german_nation['N_NATIONKEY']

# Find all supplier keys from Germany in MongoDB
german_supplier_keys = list(supplier_collection.find({'S_NATIONKEY': german_nation_key}, {'S_SUPPKEY': 1}))

# Extract supplier keys to a list
german_supplier_key_list = [sup['S_SUPPKEY'] for sup in german_supplier_keys]

# Fetch the matching parts and suppliers from MySQL using the list of german supplier keys
with mysql_conn.cursor() as cursor:
    # Calculate and filter the sum of supply cost multiplied by available quantity from the partsupp table
    cursor.execute("""
        SELECT PS_PARTKEY, SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS total_value
        FROM partsupp
        WHERE PS_SUPPKEY IN (%s)
        GROUP BY PS_PARTKEY
        HAVING total_value > (
            SELECT SUM(PS_SUPPLYCOST * PS_AVAILQTY) * %s FROM partsupp WHERE PS_SUPPKEY IN (%s)
        )
        ORDER BY total_value DESC
    """ % (','.join(map(str, german_supplier_key_list)), threshold_percentage, ','.join(map(str, german_supplier_key_list))))

    results = cursor.fetchall()

# Write the query output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['PS_PARTKEY', 'TOTAL_VALUE'])
    for result in results:
        writer.writerow(result)

# Close the connections
mysql_conn.close()
mongo_client.close()
