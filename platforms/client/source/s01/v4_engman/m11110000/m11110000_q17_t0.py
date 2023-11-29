import pymysql
import pymongo
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(host='mysql', 
                             user='root', 
                             password='my-secret-pw', 
                             db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query MongoDB to find the parts
part_docs = mongo_db.part.find({'P_BRAND': 'BRAND#23', 'P_CONTAINER': 'MED BAG'})

# Extract part keys matching the criteria
matching_part_keys = [doc['P_PARTKEY'] for doc in part_docs]

# Query MySQL for the average quantity of lineitems with matching parts ordered
quantity_sql = f"""
SELECT AVG(L_QUANTITY) FROM lineitem 
WHERE L_PARTKEY IN ({','.join(str(pk) for pk in matching_part_keys)}) 
  AND L_QUANTITY < 20 / 100 * (SELECT AVG(L_QUANTITY) FROM lineitem)
"""
mysql_cursor.execute(quantity_sql)
average_quantity_result = mysql_cursor.fetchone()
average_quantity = average_quantity_result[0] if average_quantity_result else None

# Calculate the average yearly gross loss
if average_quantity is not None:
    loss_sql = f"""
    SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) / 7 AS yearly_loss
    FROM lineitem
    WHERE L_PARTKEY IN ({','.join(str(pk) for pk in matching_part_keys)}) AND L_QUANTITY < {average_quantity}
    """
    mysql_cursor.execute(loss_sql)
    yearly_loss_result = mysql_cursor.fetchone()
    yearly_loss = yearly_loss_result[0] if yearly_loss_result else 0
else:
    yearly_loss = 0

# Write the output to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['average_yearly_loss'])
    writer.writerow([yearly_loss])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
