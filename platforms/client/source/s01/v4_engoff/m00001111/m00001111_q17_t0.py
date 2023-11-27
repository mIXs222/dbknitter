import pymysql
import pymongo
import pandas as pd

# Connect to mysql
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4')

# Retrieve parts that match the condition
with mysql_conn.cursor() as cursor:
    part_query = """
        SELECT P_PARTKEY
        FROM part
        WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG';
    """
    cursor.execute(part_query)
    part_keys = [item[0] for item in cursor.fetchall()]

mysql_conn.close()

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
lineitem_collection = mongodb['lineitem']

# Compute the average quantity for the retrieved part keys
pipeline = [
    {'$match': {'L_PARTKEY': {'$in': part_keys}}},
    {'$group': {'_id': None, 'average_qty': {'$avg': '$L_QUANTITY'}}}
]
avg_qty_result = list(lineitem_collection.aggregate(pipeline))
average_qty = avg_qty_result[0]['average_qty'] if avg_qty_result else 0
small_qty_limit = average_qty * 0.2

# Calculate loss in revenue
pipeline = [
    {'$match': {'L_PARTKEY': {'$in': part_keys}, 'L_QUANTITY': {'$lt': small_qty_limit}}},
    {'$group': {'_id': None, 'avg_yearly_loss': {'$avg': {'$multiply': ['$L_EXTENDEDPRICE', 365]}}}}
]
loss_revenue_result = list(lineitem_collection.aggregate(pipeline))
avg_yearly_loss = loss_revenue_result[0]['avg_yearly_loss'] if loss_revenue_result else 0

# Write the result to query_output.csv
df = pd.DataFrame({'AverageYearlyLossRevenue': [avg_yearly_loss]})
df.to_csv('query_output.csv', index=False)

# Close MongoDB connection
mongo_client.close()
