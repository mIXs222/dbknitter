import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Function to convert shipdate in MongoDB's ISODate format to year string
def get_year_isoformat(isodate):
    return datetime.fromisoformat(str(isodate)).strftime('%Y')

try:
    with mysql_conn.cursor() as mysql_cursor:
        # MySQL query for supplier and nation
        mysql_query = """
        SELECT
            S_SUPPKEY, N_NAME, N_NATIONKEY, S_COMMENT
        FROM
            supplier JOIN nation ON S_NATIONKEY = N_NATIONKEY
        WHERE
            N_NAME = 'JAPAN' OR N_NAME = 'INDIA'
        """
        mysql_cursor.execute(mysql_query)
        suppliers_nations = {(row[0], row[1]): row[2] for row in mysql_cursor.fetchall()}

        # Filter orders in MongoDB
        orders = list(mongodb_db.orders.find({
            'O_ORDERDATE': {'$gte': datetime(1995, 1, 1), '$lte': datetime(1996, 12, 31)}
        }, {
            'O_ORDERKEY': 1, 'O_CUSTKEY': 1
        }))

        # Dict to hold order and customer key mapping
        order_custkey_mapping = {order['O_ORDERKEY']: order['O_CUSTKEY'] for order in orders}

        # Query for customers in MongoDB
        customers = mongodb_db.customer.find({
            'C_NATIONKEY': {'$in': [sn[1] for sn in suppliers_nations.values()]},
            'C_CUSTKEY': {'$in': list(order_custkey_mapping.values())}
        }, {
            'C_CUSTKEY': 1, 'C_NATIONKEY': 1
        })
        customers_nations = {c['C_CUSTKEY']: c['C_NATIONKEY'] for c in customers}

        # Query for lineitems in MongoDB
        lineitems = mongodb_db.lineitem.aggregate([{
            '$match': {
                'L_ORDERKEY': {'$in': list(order_custkey_mapping.keys())},
                'L_SHIPDATE': {'$gte': datetime(1995, 1, 1), '$lte': datetime(1996, 12, 31)}
            }
        }, {
            '$project': {
                'L_ORDERKEY': 1,
                'L_SUPPKEY': 1,
                'L_EXTENDEDPRICE': 1,
                'L_DISCOUNT': 1,
                'L_SHIPDATE': 1
            }
        }])
        
        # Join the data from MySQL and MongoDB in Python and write to CSV
        with open('query_output.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['SUPP_NATION', 'CUST_NATION', 'L_YEAR', 'REVENUE'])
            
            for item in lineitems:
                l_suppkey = item['L_SUPPKEY']
                l_orderkey = item['L_ORDERKEY']
                l_custkey = order_custkey_mapping.get(l_orderkey)
                if l_suppkey in suppliers_nations and l_custkey in customers_nations:
                    supp_nation = suppliers_nations[l_suppkey]
                    cust_nation = customers_nations.get(l_custkey)
                    if supp_nation and cust_nation:
                        volume = item['L_EXTENDEDPRICE'] * (1 - item['L_DISCOUNT'])
                        l_year = get_year_isoformat(item['L_SHIPDATE'])
                        writer.writerow([supp_nation, cust_nation, l_year, volume])
finally:
    mongodb_client.close()
    mysql_conn.close()
