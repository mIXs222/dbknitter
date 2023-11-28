import pymysql
import pymongo
import csv
from datetime import datetime

def connect_mysql():
    return pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

def connect_mongo():
    client = pymongo.MongoClient('mongodb', 27017)
    return client['tpch']

def execute_query_and_export():
    mysql_conn = connect_mysql()
    mongo_db = connect_mongo()
    
    # Table names for MongoDB
    nation_table = mongo_db.nation
    supplier_table = mongo_db.supplier
    
    # Query for MySQL
    mysql_query = '''
        SELECT c.C_NATIONKEY as customer_nation, o.O_ORDERDATE, l.L_EXTENDEDPRICE, l.L_DISCOUNT, l.L_PARTKEY, l.L_SUPPKEY, o.O_CUSTKEY
        FROM lineitem l
        JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
        JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY
        WHERE o.O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
    '''
    
    with mysql_conn.cursor() as cursor:
        cursor.execute(mysql_query)
        mysql_results = cursor.fetchall()
        
    # Find nation names for customer and supplier
    nations = {doc['N_NATIONKEY']: doc['N_NAME'] for doc in nation_table.find()}
    suppliers = {doc['S_SUPPKEY']: doc['N_NATIONKEY'] for doc in supplier_table.find()}

    # Filter nations and calculate revenues
    results = []
    for row in mysql_results:
        customer_nation = nations.get(row[0])
        order_date = row[1]
        extended_price = row[2]
        discount = row[3]
        part_key = row[4]
        supplier_nation_key = suppliers.get(row[5])
        if supplier_nation_key is None or customer_nation not in ['JAPAN', 'INDIA']:
            continue
        supplier_nation = nations.get(supplier_nation_key)
        if supplier_nation not in ['JAPAN', 'INDIA']:
            continue
        if supplier_nation == customer_nation:
            continue
        revenue = extended_price * (1 - discount)
        year = order_date.year
        results.append((supplier_nation, customer_nation, year, revenue))
    
    # Group by supplier nation, customer nation, and year
    grouped_results = {}
    for supp_nation, cust_nation, year, rev in results:
        key = (supp_nation, cust_nation, year)
        if key not in grouped_results:
            grouped_results[key] = 0
        grouped_results[key] += rev
    
    sorted_results = sorted(grouped_results.keys())
    
    # Write results to a file
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR', 'REVENUE'])
        for res in sorted_results:
            csvwriter.writerow([res[0], res[1], res[2], grouped_results[res]])

    mysql_conn.close()

if __name__ == '__main__':
    execute_query_and_export()
