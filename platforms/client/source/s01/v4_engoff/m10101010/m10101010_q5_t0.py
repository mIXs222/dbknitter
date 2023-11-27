import pymysql
import pymongo
import csv
from datetime import datetime

# Helper function to calculate revenue volume
def calc_revenue(lineitems):
    return sum([l['L_EXTENDEDPRICE'] * (1 - l['L_DISCOUNT']) for l in lineitems])

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Query ASIA region key from MySQL
query_region = "SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA';"
mysql_cursor.execute(query_region)
asia_region_key = mysql_cursor.fetchone()
if asia_region_key is None:
    raise Exception("ASIA region not found in MySQL database.")
else:
    asia_region_key = asia_region_key[0]
    
# Query nations in the ASIA region from MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
asian_nations = list(mongo_db.nation.find({'N_REGIONKEY': asia_region_key}, {'N_NATIONKEY': 1, 'N_NAME': 1}))

# Date range
start_date = datetime(1990, 1, 1)
end_date = datetime(1995, 1, 1)

# Iterate nations and calculate revenue
nation_revenue = {}
for nation in asian_nations:
    nation_key = nation['N_NATIONKEY']
    nation_name = nation['N_NAME']
    
    # Get suppliers in the current nation from MySQL
    query_supplier = f"SELECT S_SUPPKEY FROM supplier WHERE S_NATIONKEY = {nation_key};"
    mysql_cursor.execute(query_supplier)
    suppliers = mysql_cursor.fetchall()
    supplier_keys = [s[0] for s in suppliers]

    # Get customers in the current nation from MySQL
    query_customer = f"SELECT C_CUSTKEY FROM customer WHERE C_NATIONKEY = {nation_key};"
    mysql_cursor.execute(query_customer)
    customers = mysql_cursor.fetchall()
    customer_keys = [c[0] for c in customers]

    # Get lineitems from MySQL matching suppliers and customers
    query_lineitem = "SELECT L_EXTENDEDPRICE, L_DISCOUNT FROM lineitem WHERE " \
                     f"L_SUPPKEY IN ({','.join(map(str, supplier_keys))}) AND " \
                     f"L_ORDERKEY IN (SELECT O_ORDERKEY FROM orders WHERE O_CUSTKEY IN ({','.join(map(str, customer_keys))}) AND " \
                     f"O_ORDERDATE >= '{start_date.strftime('%Y-%m-%d')}' AND O_ORDERDATE < '{end_date.strftime('%Y-%m-%d')}');"
    mysql_cursor.execute(query_lineitem)
    lineitems = mysql_cursor.fetchall()
    nation_revenue[nation_name] = calc_revenue([{'L_EXTENDEDPRICE': li[0], 'L_DISCOUNT': li[1]} for li in lineitems])

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Sort nations by revenue and output to CSV file
sorted_nations = sorted(nation_revenue.items(), key=lambda item: item[1], reverse=True)
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['NATION', 'REVENUE'])
    for nation, revenue in sorted_nations:
        csvwriter.writerow([nation, revenue])
