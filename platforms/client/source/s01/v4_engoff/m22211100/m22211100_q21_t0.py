import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query orders in MySQL
mysql_cursor.execute("""
SELECT O_ORDERKEY
FROM orders
WHERE O_ORDERSTATUS = 'F'
""")
orders_with_status_f = {order[0] for order in mysql_cursor.fetchall()}

# Query lineitem in MySQL
mysql_cursor.execute("""
SELECT L_ORDERKEY, L_COMMITDATE, L_RECEIPTDATE, L_SUPPKEY
FROM lineitem
""")
lineitems = mysql_cursor.fetchall()

# Filter lineitems for multi-supplier orders with late delivery from a single supplier
late_supplier_orders = set()
for lineitem in lineitems:
    order_key, commit_date, receipt_date, supp_key = lineitem
    if order_key in orders_with_status_f and receipt_date > commit_date:
        late_supplier_orders.add((order_key, supp_key))

mysql_cursor.close()
mysql_conn.close()

# Query nation in Redis
nation_data = redis_client.get('nation')
nation_df = pd.read_json(nation_data)
saudi_nationkey = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']['N_NATIONKEY'].iloc[0]

# Query supplier in MongoDB to find ones who kept orders waiting
suppliers = mongo_db['supplier'].find({'S_NATIONKEY': saudi_nationkey})
sa_supplier_keys = {supplier['S_SUPPKEY'] for supplier in suppliers if supplier['S_SUPPKEY'] in {supp_key for _, supp_key in late_supplier_orders}}

# Filter out orders that have only one late supplier and it's from Saudi Arabia
single_late_suppliers = {order_key for order_key, supp_key in late_supplier_orders if supp_key in sa_supplier_keys}

# Build final DataFrame
final_supplier_info = [{
    'supplier_id': supp_key
} for order_key, supp_key in late_supplier_orders if order_key in single_late_suppliers]

final_df = pd.DataFrame(final_supplier_info)

# Save output to CSV
final_df.to_csv('query_output.csv', index=False)
