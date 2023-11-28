import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL (mysql)
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Get customer data
customer_query = "SELECT * FROM customer"
df_customers = pd.read_sql(customer_query, mysql_conn)
mysql_conn.close()

# Connect to MongoDB (mongodb)
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
mongo_lineitem_collection = mongo_db["lineitem"]
lineitems = list(mongo_lineitem_collection.find({}, {'_id': 0}))
df_lineitems = pd.DataFrame(lineitems)
mongo_client.close()

# Connect to Redis (redis) and get orders data
redis_conn = DirectRedis(host='redis', port=6379, db=0)
df_orders = pd.read_json(redis_conn.get('orders').decode('utf-8'))

# Aggregate the total quantity by order key in lineitems
total_quantity_by_order_key = df_lineitems.groupby('L_ORDERKEY').agg({'L_QUANTITY': 'sum'}).reset_index()
# Filter orders with total quantity > 300
orders_with_quantity_over_300 = total_quantity_by_order_key[total_quantity_by_order_key['L_QUANTITY'] > 300]['L_ORDERKEY']

# Filter orders in df_orders based on the order keys obtained
filtered_orders = df_orders[df_orders['O_ORDERKEY'].isin(orders_with_quantity_over_300)]

# Merge the customer and orders on customer key
df_merged = pd.merge(filtered_orders, df_customers, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Add the total quantity to the merged dataframe
df_final = pd.merge(df_merged, total_quantity_by_order_key, how='left', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Select and rename the columns as per the given requirements
final_columns = {
    'C_NAME': 'Customer Name',
    'C_CUSTKEY': 'Customer Key',
    'O_ORDERKEY': 'Order Key',
    'O_ORDERDATE': 'Order Date',
    'O_TOTALPRICE': 'Total Price',
    'L_QUANTITY': 'Total Quantity',
}
df_output = df_final[list(final_columns.keys())].rename(columns=final_columns)

# Sort the results as per the given requirements
df_output_sorted = df_output.sort_values(by=['Total Price', 'Order Date'], ascending=[False, True])

# Write the output to a CSV file
df_output_sorted.to_csv('query_output.csv', index=False)
