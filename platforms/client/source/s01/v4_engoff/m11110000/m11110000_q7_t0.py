import csv
import pymysql
import pymongo
from datetime import datetime

def get_supplier_customer_nation_mapping(mongo_connection_info):
    client = pymongo.MongoClient(host=mongo_connection_info['hostname'], port=mongo_connection_info['port'])
    db = client[mongo_connection_info['database']]
    
    supplier_nations = {}
    customer_nations = {}
    
    for nation in db.nation.find({"N_NAME": {"$in": ["INDIA", "JAPAN"]}}):
        for supplier in db.supplier.find({"S_NATIONKEY": nation["N_NATIONKEY"]}):
            supplier_nations[supplier["S_SUPPKEY"]] = nation["N_NAME"]
        
        # Assuming customer collection corresponds to customers in the nation collection
        customer_nations[nation["N_NATIONKEY"]] = nation["N_NAME"]
    
    client.close()
    
    return supplier_nations, customer_nations

def run_query(mysql_connection_info, supplier_nations, customer_nations):
    connection = pymysql.connect(host=mysql_connection_info['hostname'],
                                 user=mysql_connection_info['username'],
                                 password=mysql_connection_info['password'],
                                 database=mysql_connection_info['database'])
    
    cursor = connection.cursor()
    
    result = []
    
    query = """
    SELECT s.S_NAME, c.C_NAME, YEAR(l.L_SHIPDATE) as year, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as revenue
    FROM lineitem l
    JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
    JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY
    JOIN supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
    WHERE l.L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31' AND s.S_NATIONKEY = c.C_NATIONKEY AND s.S_SUPPKEY IN ({})
    GROUP BY s.S_NAME, c.C_NAME, year
    ORDER BY s.S_NAME, c.C_NAME, year
    """
    # Format query with list of supplier keys belonging to INDIA and JAPAN
    supplier_keys = ', '.join(map(str, supplier_nations.keys()))
    formatted_query = query.format(supplier_keys)
    
    cursor.execute(formatted_query)
    
    for row in cursor:
        supp_nation = supplier_nations.get(row[0], None) # Use supplier name to get nation
        cust_nation = customer_nations.get(row[1], None) # Use customer nation key to get nation
        if supp_nation and cust_nation:
            result.append((supp_nation, cust_nation, row[2], row[3]))
    
    cursor.close()
    connection.close()
    
    return result

def write_to_csv(results, output_file):
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Supplier Nation', 'Customer Nation', 'Year', 'Revenue'])
        for row in results:
            writer.writerow(row)

# Connection information
mysql_connection_info = {
    'database': 'tpch',
    'username': 'root',
    'password': 'my-secret-pw',
    'hostname': 'mysql'
}

mongo_connection_info = {
    'database': 'tpch',
    'port': 27017,
    'hostname': 'mongodb'
}

supplier_nations, customer_nations = get_supplier_customer_nation_mapping(mongo_connection_info)
results = run_query(mysql_connection_info, supplier_nations, customer_nations)
write_to_csv(results, 'query_output.csv')
