import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
nation_coll = mongo_db['nation']
supplier_coll = mongo_db['supplier']
orders_coll = mongo_db['orders']

# Retrieve nations with the name 'SAUDI ARABIA'
saudi_arabia_nations = list(nation_coll.find({'N_NAME': 'SAUDI ARABIA'}, {'_id': 0, 'N_NATIONKEY': 1}))

# Retrieve suppliers from Saudi Arabia
saudi_arabia_nation_keys = [n['N_NATIONKEY'] for n in saudi_arabia_nations]
saudi_arabia_suppliers = list(supplier_coll.find({'S_NATIONKEY': {'$in': saudi_arabia_nation_keys}},
                                                  {'_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1}))

# Retrieve orders with status 'F'
orders_f = list(orders_coll.find({'O_ORDERSTATUS': 'F'}, {'_id': 0, 'O_ORDERKEY': 1}))

# Extract order keys.
order_keys_f = [order['O_ORDERKEY'] for order in orders_f]

# Compile the results
results = []

try:
    for supplier in saudi_arabia_suppliers:
        s_suppkey = supplier['S_SUPPKEY']
        s_name = supplier['S_NAME']

        # Query for the lineitems related to the supplier and the orders with status 'F'
        mysql_cursor.execute("""
        SELECT COUNT(*) AS NUMWAIT FROM lineitem AS L1
        WHERE L1.L_SUPPKEY = %s
        AND L1.L_ORDERKEY IN %s
        AND L1.L_RECEIPTDATE > L1.L_COMMITDATE
        AND EXISTS (
            SELECT * FROM lineitem AS L2
            WHERE L2.L_ORDERKEY = L1.L_ORDERKEY
            AND L2.L_SUPPKEY <> L1.L_SUPPKEY
        )
        AND NOT EXISTS (
            SELECT * FROM lineitem AS L3
            WHERE L3.L_ORDERKEY = L1.L_ORDERKEY
            AND L3.L_SUPPKEY <> L1.L_SUPPKEY
            AND L3.L_RECEIPTDATE > L3.L_COMMITDATE
        )
        """, (s_suppkey, order_keys_f))

        numwait = mysql_cursor.fetchone()[0]

        if numwait:  # If there are any results
            results.append([s_name, numwait])

    # Close MySQL cursor and connection
    mysql_cursor.close()
    mysql_conn.close()

    # Writing results to CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['S_NAME', 'NUMWAIT'])  # Header
        for result in sorted(results, key=lambda x: (-x[1], x[0])):  # Ordering by NUMWAIT DESC, S_NAME
            csv_writer.writerow(result)

except Exception as e:
    print(f'An error occurred: {e}')
