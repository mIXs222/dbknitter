# python_code.py
import pymysql
import pymongo
import csv

# Connect to the MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mycursor = mysql_conn.cursor()

# Connect to the MongoDB database
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
parts_collection = mongo_db["part"]

# Find the average quantity for parts with P_BRAND = 'Brand#23' and P_CONTAINER = 'MED BAG'
brand_parts = parts_collection.find({"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"})
part_keys = [p["P_PARTKEY"] for p in brand_parts]

# Calculate average lineitem quantity for orders of these parts
if part_keys:
    placeholders = ','.join(map(str, part_keys))
    mycursor.execute(f"""
    SELECT 
        AVG(L_QUANTITY) AS avg_quantity 
    FROM 
        lineitem 
    WHERE 
        L_PARTKEY IN ({placeholders})
    """)
    avg_quantity_result = mycursor.fetchone()
    avg_quantity = avg_quantity_result[0] if avg_quantity_result else 0
else:
    avg_quantity = 0

# Calculate average yearly revenue loss for orders with quantity less than 20% of this average
if avg_quantity > 0:
    threshold_quantity = avg_quantity * 0.2
    mycursor.execute(f"""
    SELECT 
        (SUM(L_EXTENDEDPRICE)/7) AS avg_yearly_loss 
    FROM 
        lineitem 
    WHERE 
        L_QUANTITY < {threshold_quantity}
        AND L_PARTKEY IN ({placeholders})
    """)
    avg_yearly_loss_result = mycursor.fetchone()
    avg_yearly_loss = avg_yearly_loss_result[0] if avg_yearly_loss_result else 0
else:
    avg_yearly_loss = 0

# Disconnect from the MySQL database
mysql_conn.close()

# Write to CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['avg_yearly_loss'])
    writer.writerow([avg_yearly_loss])

print(f"The output has been written to query_output.csv with an average yearly loss of: {avg_yearly_loss}")
