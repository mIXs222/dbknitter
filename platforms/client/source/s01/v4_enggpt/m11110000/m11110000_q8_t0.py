import pymysql
import pymongo
from datetime import datetime
import csv

# Establish connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Get data needed from MongoDB
nation_collection = mongodb['nation']
region_collection = mongodb['region']
part_collection = mongodb['part']

# Define Asia region key and get the nations in Asia, specifically India's key
asia_key = region_collection.find_one({'R_NAME': 'ASIA'})['R_REGIONKEY']
india_key = nation_collection.find_one({'N_NAME': 'INDIA', 'N_REGIONKEY': asia_key})['N_NATIONKEY']

# Get the part keys for 'SMALL PLATED COPPER'
part_keys = [p['P_PARTKEY'] for p in part_collection.find({'P_TYPE': 'SMALL PLATED COPPER'})]

# Prepare SQL queries
lineitem_query = """
SELECT
    L_ORDERKEY, L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
FROM
    lineitem
WHERE
    L_PARTKEY IN (%s)
"""

orders_query = """
SELECT
    O_ORDERKEY, O_CUSTKEY, YEAR(O_ORDERDATE) as year
FROM
    orders
WHERE
    YEAR(O_ORDERDATE) >= 1995 AND YEAR(O_ORDERDATE) <= 1996
"""

customer_query = """
SELECT
    C_CUSTKEY, C_NATIONKEY
FROM
    customer
WHERE
    C_NATIONKEY = %s
"""

with mysql_conn.cursor() as cursor:
    # Get lineitems for parts of interest
    params = ','.join(['%s'] * len(part_keys))
    cursor.execute(lineitem_query % params, part_keys)
    lineitem_data = cursor.fetchall()

    # Create a map of order keys to line items
    lineitem_map = {}
    for orderkey, partkey, extendedprice, discount, shipdate in lineitem_data:
        if partkey in part_keys:
            volume = extendedprice * (1 - discount)
            if orderkey not in lineitem_map:
                lineitem_map[orderkey] = []
            lineitem_map[orderkey].append(volume)

    # Get orders in the years of interest
    cursor.execute(orders_query)
    orders_data = cursor.fetchall()
    
    # Build a map of custkey to year from orders
    orders_map = {orderkey: year for orderkey, custkey, year in orders_data if orderkey in lineitem_map}

    # Get customers from INDIA
    cursor.execute(customer_query, india_key)
    customer_data = cursor.fetchall()

    # Build a set of customers from INDIA
    customers_india = {custkey for custkey, nationkey in customer_data}

# Filter the orders for customers from INDIA
india_orders = {orderkey for orderkey, year in orders_map.items() if orderkey in customers_india}

# Calculate the total volume for each year
years_volume = {year: sum(lineitem_map[orderkey]) for orderkey, year in orders_map.items() if orderkey in india_orders}

# Calculate market share
total_volume = sum(years_volume.values())
market_share = {year: volume / total_volume for year, volume in years_volume.items()}

# Write results to CSV
csv_headers = ['Year', 'Market Share']
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(csv_headers)
    for year in sorted(market_share):
        writer.writerow([year, market_share[year]])

# Closing connections
mysql_conn.close()
mongo_client.close()
