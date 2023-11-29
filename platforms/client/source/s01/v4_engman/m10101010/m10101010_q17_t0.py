import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch',
    charset='utf8mb4'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
part_collection = mongo_db["part"]

# Get the average lineitem quantity of BRAND#23 and MED BAG from MySQL
mysql_query = """
    SELECT AVG(l.L_QUANTITY) AS avg_quantity
    FROM lineitem l 
    INNER JOIN part p ON l.L_PARTKEY = p.P_PARTKEY
    WHERE p.P_BRAND = 'BRAND#23' AND p.P_CONTAINER = 'MED BAG';
"""
mysql_cursor.execute(mysql_query)
avg_quantity = mysql_cursor.fetchone()[0]

# Calculate 20% of the average quantity
threshold_quantity = avg_quantity * 0.20

# Get the yearly loss from MySQL
mysql_query = """
    SELECT YEAR(l.L_SHIPDATE) AS year, SUM(l.L_EXTENDEDPRICE * l.L_DISCOUNT) AS loss
    FROM lineitem l 
    INNER JOIN part p ON l.L_PARTKEY = p.P_PARTKEY
    WHERE p.P_BRAND = 'BRAND#23' AND 
          p.P_CONTAINER = 'MED BAG' AND 
          l.L_QUANTITY < %s
    GROUP BY YEAR(l.L_SHIPDATE);
"""
mysql_cursor.execute(mysql_query, (threshold_quantity,))
yearly_loss = mysql_cursor.fetchall()

# Calculate the average yearly gross loss
total_loss = sum(loss for _, loss in yearly_loss)
average_yearly_loss = total_loss / len(yearly_loss)
result = [("Average Yearly Loss", average_yearly_loss)]

# Write the result to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Average Yearly Loss'])
    writer.writerow([average_yearly_loss])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
