import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL server and select data from customer table
mysql_conn = pymysql.connect(
    host='mysql', user='root', password='my-secret-pw', database='tpch'
)
mysql_query = """
SELECT C_CUSTKEY, C_NAME
FROM customer;
"""
customers = pd.read_sql(mysql_query, mysql_conn)

# Connect to MongoDB server and select data from orders table
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']
orders = pd.DataFrame(list(orders_collection.find({}, {'_id': 0, 'O_ORDERKEY': 1, 'O_CUSTKEY': 1, 'O_ORDERDATE': 1, 'O_TOTALPRICE': 1})))

# Connect to Redis and select data from lineitem table
redis = DirectRedis(host='redis', port=6379, db=0)
lineitem = pd.DataFrame(eval(redis.get('lineitem')))

# group lineitem by L_ORDERKEY and filter out the ones with SUM(L_QUANTITY) <= 300
lineitem_grouped = lineitem.groupby('L_ORDERKEY').filter(lambda x: x['L_QUANTITY'].sum() > 300)

# merge customers and orders on customer key
merged_df = pd.merge(customers, orders, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# merge the result with lineitems on order key
final_merged_df = pd.merge(merged_df, lineitem_grouped, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# calculate the sum of L_QUANTITY for each group and sort the result as indicated in the query
final_result = final_merged_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).agg({
    'L_QUANTITY': 'sum'
}).reset_index()

final_result_sorted = final_result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# write to CSV
final_result_sorted.to_csv('query_output.csv', index=False)
mysql_conn.close()
