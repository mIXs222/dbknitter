import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Get ASIA region key
mysql_cursor.execute("SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA'")
asia_region_key = mysql_cursor.fetchone()[0]

# Get nation keys for nations in ASIA
mysql_cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_REGIONKEY = %s", (asia_region_key,))
nations_in_asia = {row[0]: row[1] for row in mysql_cursor.fetchall()}
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client['tpch']

# Find orders within date range
orders_within_range = mongodb['orders'].find({
    'O_ORDERDATE': {'$gte': '1990-01-01', '$lt': '1995-01-01'}
}, {'O_ORDERKEY': 1})

order_keys = [order['O_ORDERKEY'] for order in orders_within_range]

# Get lineitems for orders within date range
lineitems = list(mongodb['lineitem'].find({
    'L_ORDERKEY': {'$in': order_keys}
}, {'L_ORDERKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1}))

# Map order keys to lineitems
orderkey_to_lineitems = {}
for lineitem in lineitems:
    if lineitem['L_ORDERKEY'] not in orderkey_to_lineitems:
        orderkey_to_lineitems[lineitem['L_ORDERKEY']] = []
    revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
    orderkey_to_lineitems[lineitem['L_ORDERKEY']].append(revenue)

mongo_client.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis
suppliers_df = pd.read_json(redis_conn.get('supplier').decode('utf-8'))
customers_df = pd.read_json(redis_conn.get('customer').decode('utf-8'))

# Filter suppliers and customers within ASIA
suppliers_in_asia = suppliers_df[suppliers_df['S_NATIONKEY'].isin(nations_in_asia.keys())]
customers_in_asia = customers_df[customers_df['C_NATIONKEY'].isin(nations_in_asia.keys())]

# Compute the revenue volume by nation
revenue_by_nation = {}
for index, customer in customers_in_asia.iterrows():
    customer_orders = [key for key in orderkey_to_lineitems.keys() if key.startswith(str(customer['C_CUSTKEY']))]
    for order_key in customer_orders:
        nation_key = customer['C_NATIONKEY']
        nation_name = nations_in_asia[nation_key]
        revenue = sum(orderkey_to_lineitems[order_key])
        if nation_name not in revenue_by_nation:
            revenue_by_nation[nation_name] = 0
        revenue_by_nation[nation_name] += revenue

# Create DataFrame from results and write to CSV
result_df = pd.DataFrame(revenue_by_nation.items(), columns=['N_NAME', 'REVENUE']).sort_values(by='REVENUE', ascending=False)
result_df.to_csv('query_output.csv', index=False)
