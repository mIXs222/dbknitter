# query.py
import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client['tpch']

# MongoDB: Retrieve suppliers located in Canada
nation_docs = list(mongodb_db.nation.find({"N_NAME": "CANADA"}, {"N_NATIONKEY": 1}))
nation_keys_canada = [doc["N_NATIONKEY"] for doc in nation_docs]

supplier_docs = list(mongodb_db.supplier.find({"S_NATIONKEY": {"$in": nation_keys_canada}},
                                              {"S_SUPPKEY": 1, "S_NAME": 1, "S_ADDRESS": 1}))

supplier_keys = [doc["S_SUPPKEY"] for doc in supplier_docs]
supplier_info = {doc["S_SUPPKEY"]: (doc["S_NAME"], doc["S_ADDRESS"]) for doc in supplier_docs}

# MySQL: Gather parts and their supply information
with mysql_connection.cursor() as cursor:
    # Subquery for parts that start with 'forest'
    cursor.execute("""
        SELECT PS_PARTKEY, PS_SUPPKEY FROM partsupp WHERE PS_PARTKEY IN (
            SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'
        )
    """)
    partsupp_info = cursor.fetchall()

    # Filter partsupp info for valid supplier keys
    valid_partsupp = [item for item in partsupp_info if item[1] in supplier_keys]

    # Prepare the IN clause for the next query
    ps_partkeys = tuple(item[0] for item in valid_partsupp) if valid_partsupp else (-1,)

    # Subquery for threshold quantity
    cursor.execute("""
        SELECT L_PARTKEY, L_SUPPKEY FROM lineitem WHERE (L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01')
        AND L_PARTKEY IN %s AND L_QUANTITY >= (
            SELECT 0.5 * SUM(L_QUANTITY) FROM lineitem WHERE L_PARTKEY = lineitem.L_PARTKEY
            AND L_SUPPKEY = lineitem.L_SUPPKEY)
    """, (ps_partkeys,))
    threshold_qty_info = cursor.fetchall()

# Close MySQL connection
mysql_connection.close()

# Filter suppliers fulfilling the conditions
final_suppliers = set(info[1] for info in threshold_qty_info) & set(supplier_keys)

# Writing results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Supplier Name', 'Supplier Address'])

    # Compose the final result, ordered by supplier name
    for supplier_key in sorted(final_suppliers):
        csvwriter.writerow([supplier_info[supplier_key][0], supplier_info[supplier_key][1]])
