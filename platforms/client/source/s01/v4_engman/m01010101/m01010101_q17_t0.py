# ExecuteQuery.py
import pymysql
import pymongo
import csv

# Function to calculate average yearly gross loss
def calculate_average_loss(avg_quantity, discounted_lineitems):
    yearly_loss = 0
    for lineitem in discounted_lineitems:
        if lineitem['L_QUANTITY'] < 0.2 * avg_quantity:
            yearly_loss += lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
            
    num_years = 7  # data for 7 years
    return yearly_loss / num_years

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Execute SQL query to get average quantity for brand#23 with 'MED BAG'
mysql_cursor.execute("""
SELECT AVG(P_SIZE) as avg_size
FROM part
WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG';
""")
avg_size_result = mysql_cursor.fetchone()
avg_quantity = avg_size_result[0]

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Get lineitems for the queried parts in the SQL DBMS from MongoDB
lineitems = mongodb_db.lineitem.find({
    'L_PARTKEY': {'$exists': True},
    'L_EXTENDEDPRICE': {'$exists': True},
    'L_DISCOUNT': {'$exists': True},
    'L_QUANTITY': {'$exists': True}
})

# Find the average yearly gross loss
average_loss = calculate_average_loss(avg_quantity, lineitems)

# Write output to CSV
with open('query_output.csv', 'w', newline='') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(['Average_Yearly_Loss'])
    csv_writer.writerow([average_loss])

# Close MongoDB connection
mongodb_client.close()
