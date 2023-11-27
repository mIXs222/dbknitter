import pymysql
import pymongo
import csv
import datetime

# Constants for specifying the product line to analyze
PRODUCT_LINE_DIM = 'dim'  # replace 'dim' with the actual dimension of the product you are interested in
YEAR = datetime.datetime.now().year  # The current year, can be changed to the desired year

# Connect to MySQL (assuming pymysql has been installed)
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Execute MySQL query
with mysql_conn.cursor() as mysql_cursor:
    mysql_query = """
    SELECT 
        S_NATIONKEY,
        YEAR(L_SHIPDATE) AS year,
        L_EXTENDEDPRICE,
        L_DISCOUNT,
        L_QUANTITY,
        L_PARTKEY,
        S_SUPPKEY
    FROM lineitem, supplier
    WHERE
        lineitem.L_SUPPKEY = supplier.S_SUPPKEY
        AND YEAR(L_SHIPDATE) = %s;
    """
    mysql_cursor.execute(mysql_query, (YEAR,))
    mysql_results = mysql_cursor.fetchall()

# Close MySQL connection
mysql_conn.close()

# Connect to MongoDB (assuming pymongo has been installed)
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client['tpch']

# Execute MongoDB queries
part_docs = list(mongodb.part.find({"P_NAME": {"$regex": ".*{}.*".format(PRODUCT_LINE_DIM)}}, {"P_PARTKEY": 1}))
partkeys = [doc['P_PARTKEY'] for doc in part_docs]

partsupp_docs = mongodb.partsupp.find(
    {"PS_PARTKEY": {"$in": partkeys}},
    {"PS_PARTKEY": 1, "PS_SUPPKEY": 1, "PS_SUPPLYCOST": 1}
)
partsupp_mapping = {(doc['PS_PARTKEY'], doc['PS_SUPPKEY']): doc['PS_SUPPLYCOST'] for doc in partsupp_docs}

# Prepare the result dictionary
results = {}

# Processing results from MySQL
for row in mysql_results:
    nation_key, year, extendedprice, discount, quantity, part_key, supp_key = row
    supply_cost = partsupp_mapping.get((part_key, supp_key), 0)
    profit = (extendedprice * (1 - discount)) - (supply_cost * quantity)
    
    if nation_key not in results:
        results[nation_key] = {}
    if year not in results[nation_key]:
        results[nation_key][year] = 0
    results[nation_key][year] += profit

# Get nation names from MongoDB
nations = {doc['N_NATIONKEY']: doc['N_NAME'] for doc in mongodb.nation.find({}, {'N_NATIONKEY': 1, 'N_NAME': 1})}

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['NATION', 'YEAR', 'PROFIT'])
    
    for nation_key, year_profits in results.items():
        nation_name = nations.get(nation_key, 'UNKNOWN')
        for year, profit in sorted(year_profits.items(), key=lambda x: x[0], reverse=True):
            csvwriter.writerow([nation_name, year, profit])

# Close MongoDB connection
mongo_client.close()
