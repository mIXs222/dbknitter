import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client['tpch']

# MongoDB query for nation and part data
parts = mongo_db.part.find({'P_TYPE': 'SMALL PLATED COPPER'})
parts_dict = {part['P_PARTKEY']: part for part in parts}
nations = mongo_db.nation.find({'N_NAME': 'INDIA', 'N_REGIONKEY': {'$exists': True}})
nation_dict = {nation['N_NATIONKEY']: nation for nation in nations}

# Get region key for 'ASIA'
mysql_cursor.execute("SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA'")
asia_region = mysql_cursor.fetchone()
if not asia_region:
    raise Exception("Region 'ASIA' not found")
asia_region_key = asia_region[0]

# Get nation key and customer keys for 'INDIA' in 'ASIA'
mysql_cursor.execute("""
    SELECT N_NATIONKEY
    FROM nation
    WHERE N_NAME = 'INDIA' AND N_REGIONKEY = %s
""", (asia_region_key,))
india_nation_key = mysql_cursor.fetchone()
if not india_nation_key:
    raise Exception("Nation 'INDIA' in region 'ASIA' not found")
india_nation_key = india_nation_key[0]

# PART, LINEITEM, ORDERS SQL query. We shall join these in the csv no SQL query as mongo doesn't support SQL
orders_1995_1996 = mongo_db.orders.find({
    'O_ORDERDATE': {
        '$gte': datetime(1995, 1, 1),
        '$lt': datetime(1997, 1, 1)
    }
})

# Calculate market share
results = {}
for order in orders_1995_1996:
    mysql_cursor.execute("""
        SELECT
            L_EXTENDEDPRICE, L_DISCOUNT, L_PARTKEY, L_SUPPKEY
        FROM
            lineitem
        WHERE
            L_ORDERKEY = %s
    """, (order['O_ORDERKEY'],))
    
    for lineitem in mysql_cursor:
        if lineitem[2] not in parts_dict:
            continue
        volume = lineitem[0] * (1 - lineitem[1])
        supplier_nation_key = lineitem[3]
        
        if supplier_nation_key in nation_dict:
            year = order['O_ORDERDATE'].year
            if year not in results:
                results[year] = {'INDIA_VOLUME': 0, 'TOTAL_VOLUME': 0}
            results[year]['INDIA_VOLUME'] += volume
        results[year]['TOTAL_VOLUME'] += volume

for year, values in results.items():
    results[year]['MARKET_SHARE'] = values['INDIA_VOLUME'] / values['TOTAL_VOLUME']

# Close MySQL connection
mysql_conn.close()

# Write results to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["YEAR", "MARKET_SHARE"])
    
    for year in sorted(results):
        writer.writerow([year, results[year]['MARKET_SHARE']])
