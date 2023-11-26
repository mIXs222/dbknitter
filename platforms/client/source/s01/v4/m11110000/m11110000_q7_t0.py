import pymysql
import pymongo
import csv
import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch')

mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Fetching nation data from MongoDB
nation_data = {n['N_NATIONKEY']: n for n in mongo_db.nation.find()}

# Fetch the relevant data from MySQL and combine with the nation data
mysql_cursor.execute("""SELECT
                            C_CUSTKEY, C_NATIONKEY,
                            O_ORDERKEY, O_CUSTKEY,
                            L_ORDERKEY, L_SUPPKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
                        FROM customer
                        INNER JOIN orders ON C_CUSTKEY = O_CUSTKEY
                        INNER JOIN lineitem ON O_ORDERKEY = L_ORDERKEY
                        WHERE L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'""")

# Structure to hold the combined data
combined_data = []

# Iterating through MySQL data and building the combined dataset
for record in mysql_cursor:
    c_custkey, c_nationkey, o_orderkey, o_custkey, l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate = record
    supplier_cursor = mongo_db.supplier.find_one({'S_SUPPKEY': l_suppkey})
    if not supplier_cursor:
        continue
    s_nationkey = supplier_cursor['S_NATIONKEY']
    if s_nationkey not in nation_data or c_nationkey not in nation_data:
        continue
    supp_nation = nation_data[s_nationkey]['N_NAME']
    cust_nation = nation_data[c_nationkey]['N_NAME']
    if (supp_nation == 'JAPAN' and cust_nation == 'INDIA') or (supp_nation == 'INDIA' and cust_nation == 'JAPAN'):
        l_year = l_shipdate.year
        volume = l_extendedprice * (1 - l_discount)
        combined_data.append((supp_nation, cust_nation, str(l_year), volume))

# Compute the revenue
revenue_data = {}
for supp_nation, cust_nation, l_year, volume in combined_data:
    key = (supp_nation, cust_nation, l_year)
    if key not in revenue_data:
        revenue_data[key] = 0
    revenue_data[key] += volume

# Sorting the data based on the specifications
sorted_revenue_data = sorted(revenue_data.items(), key=lambda x: (x[0][0], x[0][1], x[0][2]))

# Writing the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['SUPP_NATION', 'CUST_NATION', 'L_YEAR', 'REVENUE'])
    for key, revenue in sorted_revenue_data:
        csvwriter.writerow([key[0], key[1], key[2], round(revenue, 2)])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
