import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Get the Asia region key
asia_region_sql = "SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA'"
mysql_cursor.execute(asia_region_sql)
asia_region_key = mysql_cursor.fetchone()[0]

# Get nation keys in Asia region
nation_sql = f"SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_REGIONKEY = {asia_region_key}"
mysql_cursor.execute(nation_sql)
asia_nations = {key: name for key, name in mysql_cursor.fetchall()}

# Get suppliers in Asia region
supplier_sql = f"SELECT S_SUPPKEY, S_NATIONKEY FROM supplier WHERE S_NATIONKEY IN ({','.join(map(str, asia_nations.keys()))})"
mysql_cursor.execute(supplier_sql)
suppliers_in_asia = set(sup_key for sup_key, _ in mysql_cursor.fetchall())

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_tpch = mongodb_client['tpch']

# Fetch lineitem documents in the date range
lineitem_documents = mongodb_tpch['lineitem'].find({
    'L_SHIPDATE': {'$gte': '1990-01-01', '$lt': '1995-01-01'}
})

# Map of nation revenue
nation_revenue = {nation: 0 for nation in asia_nations.keys()}

# Calculate revenue
for document in lineitem_documents:
    if document['L_SUPPKEY'] in suppliers_in_asia:
        customer = mongodb_tpch['customer'].find_one({
            "C_CUSTKEY": document['L_ORDERKEY'],
            "C_NATIONKEY": {"$in": list(asia_nations.keys())}
        })
        if customer:
            revenue = document['L_EXTENDEDPRICE'] * (1 - document['L_DISCOUNT'])
            nation_revenue[customer['C_NATIONKEY']] += revenue

# Sort by revenue
sorted_nations = sorted(nation_revenue.items(), key=lambda x: x[1], reverse=True)

# Write to CSV
with open('query_output.csv', 'w', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['N_NAME', 'REVENUE'])
    for nation_key, revenue in sorted_nations:
        csv_writer.writerow([asia_nations[nation_key], revenue])

# Close MongoDB connection
mongodb_client.close()
