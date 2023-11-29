import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Helper function to get the nation key for India and region key for Asia
def get_keys():
    asia_region_key = None
    india_nation_key = None
    for region in mongodb_db.region.find({'R_NAME': 'ASIA'}):
        asia_region_key = region['R_REGIONKEY']
    
    for nation in mongodb_db.nation.find({'N_NAME': 'INDIA', 'N_REGIONKEY': asia_region_key}):
        india_nation_key = nation['N_NATIONKEY']

    return india_nation_key

india_nation_key = get_keys()

# Query MySQL for market share
mysql_cursor = mysql_conn.cursor()

# Compute market share for 1995 and 1996
market_share_years = {}
for year in [1995, 1996]:
    query = f"""
    SELECT YEAR(O_ORDERDATE) as year, 
           SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as market_share
    FROM orders, lineitem, part, supplier
    WHERE O_ORDERKEY = L_ORDERKEY
      AND L_PARTKEY = P_PARTKEY
      AND L_SUPPKEY = S_SUPPKEY
      AND P_TYPE = 'SMALL PLATED COPPER'
      AND S_NATIONKEY = {india_nation_key}
      AND YEAR(O_ORDERDATE) = {year}
    GROUP BY YEAR(O_ORDERDATE)
    """
    
    mysql_cursor.execute(query)
    result = mysql_cursor.fetchone()
    if result:
        market_share_years[year] = float(result[1])
    else:
        market_share_years[year] = 0.0

mysql_conn.close()

# Write the output to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['order_year', 'market_share'])
    for year in sorted(market_share_years.keys()):
        writer.writerow([year, market_share_years[year]])
