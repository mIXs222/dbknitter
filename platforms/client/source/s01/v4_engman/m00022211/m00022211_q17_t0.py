import pymysql
import pymongo
import pandas as pd
from statistics import mean

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch'
)

# Fetch parts with BRAND#23 and MED BAG
with mysql_conn:
    with mysql_conn.cursor() as cursor:
        query_mysql = """
        SELECT P_PARTKEY
        FROM part
        WHERE P_BRAND = 'BRAND#23' and P_CONTAINER = 'MED BAG'
        """
        cursor.execute(query_mysql)
        part_keys = [row[0] for row in cursor.fetchall()]

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
mongo_collection = mongo_db["lineitem"]

# Fetch lineitems with quantity and extended price
lineitems = list(mongo_collection.find(
    {"L_PARTKEY": {"$in": part_keys}},
    {"L_QUANTITY": 1, "L_EXTENDEDPRICE": 1}
))

# Close the connections
mysql_conn.close()
mongo_client.close()

# Compute average lineitem quantity
avg_quantity = mean([item["L_QUANTITY"] for item in lineitems])

# Compute the average yearly loss
loss_items = [item for item in lineitems if item["L_QUANTITY"] < 0.2 * avg_quantity]
avg_yearly_loss = mean([item["L_EXTENDEDPRICE"] for item in loss_items]) * (1 / 7)

# Write the output to a CSV file
df = pd.DataFrame([{"average_yearly_loss": avg_yearly_loss}])
df.to_csv('query_output.csv', index=False)
