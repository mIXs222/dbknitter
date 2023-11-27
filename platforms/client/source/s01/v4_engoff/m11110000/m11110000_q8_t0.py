# Import required libraries
import pymysql
import pymongo
import csv
from decimal import Decimal

# Connection details
mysql_connection_info = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}
mongodb_connection_info = {
    'database': 'tpch',
    'port': 27017,
    'host': 'mongodb',
}

# Connect to MySQL
mysql_conn = pymysql.connect(
    host=mysql_connection_info['host'],
    user=mysql_connection_info['user'],
    password=mysql_connection_info['password'],
    db=mysql_connection_info['database'],
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient(mongodb_connection_info['host'], mongodb_connection_info['port'])
mongodb = mongo_client[mongodb_connection_info['database']]

# MongoDB: Get N_NATIONKEY for INDIA and R_REGIONKEY for ASIA
asia_region = mongodb.region.find_one({'R_NAME': 'ASIA'})
if asia_region:
    asia_region_key = asia_region['R_REGIONKEY']
    india_nation = mongodb.nation.find_one({'N_NAME': 'INDIA', 'N_REGIONKEY': asia_region_key})
    if india_nation:
        india_nation_key = india_nation['N_NATIONKEY']

        # MongoDB: Get S_SUPPKEY for suppliers from INDIA in ASIA region
        india_suppliers = list(mongodb.supplier.find({'S_NATIONKEY': india_nation_key}, {'_id': 0, 'S_SUPPKEY': 1}))
        india_supplier_keys = [supplier['S_SUPPKEY'] for supplier in india_suppliers]

        # MySQL: Get revenues from lineitem and orders for years 1995 and 1996 for SMALL PLATED COPPER where supplier is from INDIA
        with mysql_conn.cursor() as cursor:
            query = """
                SELECT
                    YEAR(O_ORDERDATE) as year,
                    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue
                FROM
                    lineitem, orders, part, supplier
                WHERE
                    L_LINESTATUS = 'F'
                    AND L_RETURNFLAG = 'R'
                    AND O_ORDERKEY = L_ORDERKEY
                    AND P_PARTKEY = L_PARTKEY
                    AND P_TYPE = 'SMALL PLATED COPPER'
                    AND S_SUPPKEY = L_SUPPKEY
                    AND S_NATIONKEY = %s
                    AND YEAR(O_ORDERDATE) IN (1995, 1996)
                GROUP BY
                    YEAR(O_ORDERDATE)
            """
            cursor.execute(query, (india_nation_key,))
            results = cursor.fetchall()

        # Write to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['YEAR', 'MARKET_SHARE'])
            for row in results:
                writer.writerow([row['year'], str(Decimal(row['revenue']))])

mysql_conn.close()
mongo_client.close()
