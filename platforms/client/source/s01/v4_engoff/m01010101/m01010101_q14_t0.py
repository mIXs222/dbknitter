import pymysql
import pymongo
from datetime import datetime
import csv

# MySQL Connection
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

# MongoDB Connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Query part data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT P_PARTKEY, P_RETAILPRICE FROM part WHERE P_RETAILPRICE > 0")
    part_data = cursor.fetchall()

# Create dictionary for part retail prices where parts are promotional
part_retail_prices = {part[0]: part[1] for part in part_data}

# Define the date range for shipped parts
start_date = datetime.strptime('1995-09-01', '%Y-%m-%d')
end_date = datetime.strptime('1995-10-01', '%Y-%m-%d')

# Query lineitem data from MongoDB within the date range
lineitem_data = mongodb_db.lineitem.find({
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
})

# Calculate promotional revenue
promotional_revenue = 0
total_revenue = 0

for doc in lineitem_data:
    if doc['L_PARTKEY'] in part_retail_prices:
        revenue = doc['L_EXTENDEDPRICE'] * (1 - doc['L_DISCOUNT'])
        promotional_revenue += revenue
        total_revenue += revenue
    else:
        revenue = doc['L_EXTENDEDPRICE'] * (1 - doc['L_DISCOUNT'])
        total_revenue += revenue

# Calculate percentage if total_revenue is not zero
if total_revenue != 0:
    promotional_revenue_percentage = (promotional_revenue / total_revenue) * 100
else:
    promotional_revenue_percentage = 0

# Write result to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['PROMOTIONAL_REVENUE_PERCENTAGE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerow({'PROMOTIONAL_REVENUE_PERCENTAGE': promotional_revenue_percentage})

# Close connections
mysql_conn.close()
mongodb_client.close()
