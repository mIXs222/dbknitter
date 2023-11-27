import pymongo
import pymysql
import csv
from datetime import datetime

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')
mysql_cursor = mysql_connection.cursor()

# Get the relevant parts from MongoDB (parts of brand 23 with MED BAG)
part_keys = []
for part in part_collection.find({"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"}):
    part_keys.append(part['P_PARTKEY'])

# Now query the MySQL database to get the required data
lineitem_query = """
SELECT
    L_PARTKEY,
    L_QUANTITY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_SHIPDATE
FROM
    lineitem
WHERE
    L_PARTKEY IN (%s)
""" % ','.join(str(pk) for pk in part_keys)

mysql_cursor.execute(lineitem_query)
lineitem_results = mysql_cursor.fetchall()

# Calculate the average quantity and determine average yearly revenue loss
total_quantity = 0
revenue_loss = 0
count_valid = 0

for row in lineitem_results:
    ship_date = datetime.strptime(row[4], '%Y-%m-%d')
    years_difference = (datetime.now() - ship_date).days / 365.25

    if years_difference <= 7:  # Only consider orders within a 7-year range
        total_quantity += row[1]
        count_valid += 1

        if row[1] < 0.2:  # If the quantity is less than 20% of average
            revenue_loss += (row[2] - (row[2] * row[3])) / years_difference

# Avoid division by zero if no valid items are found
avg_quantity = total_quantity / count_valid if count_valid else 0
avg_yearly_revenue_loss = revenue_loss / count_valid if count_valid else 0

# Prepare the output
with open("query_output.csv", "w", newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['avg_yearly_revenue_loss'])
    writer.writerow([avg_yearly_revenue_loss])

# Close the connections
mongo_client.close()
mysql_connection.close()
