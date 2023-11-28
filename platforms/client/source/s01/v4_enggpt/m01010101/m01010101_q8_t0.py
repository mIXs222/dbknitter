import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongo_tpch_db = mongo_client['tpch']

# Query MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT
            o.O_ORDERDATE,
            l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) AS volume
        FROM
            orders o
        INNER JOIN
            lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
        INNER JOIN
            part p ON l.L_PARTKEY = p.P_PARTKEY
        INNER JOIN
            customer c ON o.O_CUSTKEY = c.C_CUSTKEY
        WHERE
            p.P_TYPE = 'SMALL PLATED COPPER'
            AND YEAR(o.O_ORDERDATE) BETWEEN 1995 AND 1996
    """)
    mysql_data = cursor.fetchall()

# Transform MySQL data to dictionary with order date as key
volumes_by_date = {}
for order_date, volume in mysql_data:
    year = order_date.year
    if year not in volumes_by_date:
        volumes_by_date[year] = {'INDIA': 0, 'total': 0}
    volumes_by_date[year]['total'] += volume

# Query MongoDB
asian_countries = list(mongo_tpch_db.region.find({'R_NAME': 'ASIA'}))
asia_nations = list(mongo_tpch_db.nation.find(
    {'N_REGIONKEY': {'$in': [country['R_REGIONKEY'] for country in asian_countries]}}
))

india_nationkey = [nation['N_NATIONKEY'] for nation in asia_nations if nation['N_NAME'] == 'INDIA'][0]
indian_customers = list(mongo_tpch_db.customer.find({'C_NATIONKEY': india_nationkey}))

for doc in indian_customers:
    customer_orders = list(mongo_tpch_db.lineitem.aggregate([
        {'$match': {
            'L_ORDERKEY': {'$in': [order['L_ORDERKEY'] for order in mongo_tpch_db.orders.find({'O_CUSTKEY': doc['C_CUSTKEY']})]},
            'L_SHIPDATE': {'$gte': datetime(1995, 1, 1), '$lte': datetime(1996, 12, 31)}
        }},
        {'$project': {
            'volume': {'$multiply': [{'$subtract': [1, '$L_DISCOUNT']}, '$L_EXTENDEDPRICE']},
            'year': {'$year': '$L_SHIPDATE'}
        }}
    ]))
    
    # Aggregate volume by year for India
    for order in customer_orders:
        year = order['year']
        volume = order['volume']
        if year in volumes_by_date and 'INDIA' in volumes_by_date[year]:
            volumes_by_date[year]['INDIA'] += volume

# Calculate market share and write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['Year', 'Market Share']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for year, volumes in sorted(volumes_by_date.items()):
        market_share = volumes['INDIA'] / volumes['total'] if volumes['total'] else 0
        writer.writerow({'Year': year, 'Market Share': market_share})

# Close connections
mysql_conn.close()
mongo_client.close()
