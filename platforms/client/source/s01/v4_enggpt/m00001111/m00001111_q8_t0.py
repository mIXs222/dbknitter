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
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mdb = mongo_client['tpch']

# Query MySQL Database
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("""
    SELECT
        nation.N_NATIONKEY,
        part.P_PARTKEY
    FROM nation
    JOIN region ON nation.N_REGIONKEY=region.R_REGIONKEY
    JOIN supplier ON nation.N_NATIONKEY=supplier.S_NATIONKEY
    JOIN part ON supplier.S_SUPPKEY=part.P_PARTKEY
    WHERE region.R_NAME = 'ASIA' AND part.P_TYPE = 'SMALL PLATED COPPER'
""")
part_supplier_nation = {row[1]: row[0] for row in mysql_cursor.fetchall()}
mysql_cursor.close()

# Query MongoDB Database
asian_orders = mdb.orders.aggregate([
    {
        '$lookup': {
            'from': 'customer',
            'localField': 'O_CUSTKEY',
            'foreignField': 'C_CUSTKEY',
            'as': 'customer_info'
        }
    },
    {'$unwind': '$customer_info'},
    {
        '$match': {
            'customer_info.C_NATIONKEY': part_supplier_nation.values(),
            'O_ORDERDATE': {'$gte': datetime(1995, 1, 1), '$lt': datetime(1997, 1, 1)}
        }
    }
])

# Calculate volumes and prepare data for CSV
order_volumes = {}
for order in asian_orders:
    lineitems = mdb.lineitem.find({'L_ORDERKEY': order['O_ORDERKEY'], 'L_PARTKEY': {'$in': list(part_supplier_nation.keys())}})
    for lineitem in lineitems:
        year = order['O_ORDERDATE'].year
        nation = part_supplier_nation[lineitem['L_PARTKEY']]
        volume = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
        if year not in order_volumes:
            order_volumes[year] = {'INDIA': 0, 'total': 0}
        if nation == 'INDIA':
            order_volumes[year]['INDIA'] += volume
        order_volumes[year]['total'] += volume

# Write the results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['YEAR', 'MARKET_SHARE'])
    for year in sorted(order_volumes.keys()):
        total_volume = order_volumes[year]['total']
        india_volume = order_volumes[year]['INDIA']
        market_share = india_volume / total_volume if total_volume else 0
        writer.writerow([year, market_share])

# Close connections
mysql_conn.close()
mongo_client.close()
