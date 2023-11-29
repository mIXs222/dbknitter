import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get data from MySQL
mysql_cursor.execute("SELECT S_SUPPKEY FROM supplier WHERE S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'INDIA' AND N_REGIONKEY IN (SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA'))")
suppliers_from_india = mysql_cursor.fetchall()
suppliers_from_india_ids = [s[0] for s in suppliers_from_india]

# Get orders and lineitems from MongoDB
orders_collection = mongo_db["orders"]
lineitems_collection = mongo_db["lineitem"]

# Get nations and regions from Redis
nation_df = pd.read_json(redis_client.get('nation'))
region_df = pd.read_json(redis_client.get('region'))
asia_region_key = region_df[region_df['R_NAME'] == 'ASIA']['R_REGIONKEY'].iloc[0]
india_nation_key = nation_df[(nation_df['N_NAME'] == 'INDIA') & (nation_df['N_REGIONKEY'] == asia_region_key)]['N_NATIONKEY'].iloc[0]

# Get part information from Redis
part_df = pd.read_json(redis_client.get('part'))
small_plated_copper = part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER']

# Aggregate results
market_share_by_year = {}

for year in ['1995', '1996']:
    orders_in_year = orders_collection.find({"O_ORDERDATE": {"$regex": year}})
    order_keys_in_year = [order["O_ORDERKEY"] for order in orders_in_year]

    lineitems_in_year = lineitems_collection.find({"L_ORDERKEY": {"$in": order_keys_in_year},
                                                   "L_SUPPKEY": {"$in": suppliers_from_india_ids},
                                                   "L_PARTKEY": {"$in": small_plated_copper['P_PARTKEY'].tolist()}})

    total_revenue = 0
    india_revenue = 0
    for lineitem in lineitems_in_year:
        total_price = lineitem["L_EXTENDEDPRICE"] * (1 - lineitem["L_DISCOUNT"])
        if lineitem["L_SUPPKEY"] in suppliers_from_india_ids:
            india_revenue += total_price
        total_revenue += total_price

    market_share_by_year[year] = india_revenue / total_revenue if total_revenue else 0

# Close database connections
mysql_conn.close()
mongo_client.close()

# Write output to CSV
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['Order Year', 'Market Share'])

    for year, market_share in market_share_by_year.items():
        writer.writerow([year, market_share])
