import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4')

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

try:
    cursor = mysql_conn.cursor()

    # Fetch lineitem table from mysql
    cursor.execute("SELECT * FROM lineitem")
    lineitems = cursor.fetchall()

    # Assigning lineitem to a dict for easy access based on L_SUPPKEY
    lineitem_dict = {}
    for lineitem in lineitems:
        if lineitem[2] not in lineitem_dict:
            lineitem_dict[lineitem[2]] = []
        lineitem_dict[lineitem[2]].append(lineitem)

    # Fetch nation and supplier table from mongodb
    nations = list(mongo_db.nation.find({"N_NAME": "SAUDI ARABIA"}))
    suppliers = list(mongo_db.supplier.find({"S_NATIONKEY": {"$in": [n['N_NATIONKEY'] for n in nations]}}))

    # Assigning supplier to a dict for easy access
    supplier_dict = {supp['S_SUPPKEY']: supp for supp in suppliers}

    # Fetch orders table from mongodb
    orders = list(mongo_db.orders.find({"O_ORDERSTATUS": "F"}))

    # Process data and collect stats
    results = []
    for order in orders:
        if order['O_ORDERKEY'] in lineitem_dict:
            for l_item in lineitem_dict[order['O_ORDERKEY']]:
                if l_item[6] and l_item[12] > l_item[11] and l_item[2] in supplier_dict:
                    # Count of wait time
                    wait_count = sum(1 for l2 in lineitem_dict[order['O_ORDERKEY']]
                                     if l2[12] is not None and l2[12] > l2[11] and l2[2] != l_item[2])

                    # Check for other suppliers condition
                    other_suppliers_exist = any(l2[2] != l_item[2] for l2 in lineitem_dict[order['O_ORDERKEY']])

                    if other_suppliers_exist and wait_count > 0:
                        results.append((supplier_dict[l_item[2]]['S_NAME'], wait_count))

    # Remove duplicates and sort the results
    results = list(set(results))
    results.sort(key=lambda x: (-x[1], x[0]))

    # Write results to a CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['S_NAME', 'NUMWAIT'])
        for result in results:
            csv_writer.writerow(result)

finally:
    mysql_conn.close()
    mongo_client.close()
