import pymysql
import pymongo
import csv

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Perform the SQL part of the query
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT
            C_CUSTKEY,
            S_NATIONKEY,
            S_SUPPKEY,
            C_NATIONKEY,
            L_ORDERKEY,
            L_EXTENDEDPRICE,
            L_DISCOUNT
        FROM
            customer, lineitem, supplier
        WHERE
            C_NATIONKEY = S_NATIONKEY
            AND L_SUPPKEY = S_SUPPKEY
    """)
    mysql_result = cursor.fetchall()

# Fetch the nation and region data from MongoDB
nation_docs = list(mongo_db.nation.find({'N_NAME': 'ASIA'}))
region_docs = list(mongo_db.region.find({}))

# Extract the region keys from nation data
nation_regionkeys = {doc['N_NATIONKEY'] for doc in nation_docs if doc.get('N_REGIONKEY') in [r['R_REGIONKEY'] for r in region_docs]}

# Filter MySQL results for relevant nation keys
filtered_mysql_result = [row for row in mysql_result if row[1] in nation_regionkeys]

# Create a dictionary to map order keys to nation keys and a dictionary for revenue calculation
orderkey_nation_map = {}
revenue_map = {doc['N_NAME']: 0 for doc in nation_docs}

# Fetch the order data from MongoDB
orders_docs = mongo_db.orders.find({
    'O_ORDERSTATUS': {'$exists': True},
    'O_ORDERDATE': {'$gte': '1990-01-01', '$lt': '1995-01-01'}
})

# Process order documents
for doc in orders_docs:
    orderkey_nation_map[doc['O_ORDERKEY']] = doc['O_CUSTKEY']

# Combine data and calculate revenue
for row in filtered_mysql_result:
    if row[4] in orderkey_nation_map:
        nation_key = orderkey_nation_map[row[4]]
        extended_price = row[5]
        discount = row[6]
        for nation in nation_docs:
            if nation['N_NATIONKEY'] == nation_key:
                revenue_map[nation['N_NAME']] += extended_price * (1 - discount)

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['N_NAME', 'REVENUE'])
    for nation, revenue in sorted(revenue_map.items(), key=lambda x: x[1], reverse=True):
        writer.writerow([nation, revenue])

# Close the connections
mysql_conn.close()
