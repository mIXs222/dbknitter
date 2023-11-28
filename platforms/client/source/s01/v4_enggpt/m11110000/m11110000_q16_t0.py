# python_code.py

import csv
import pymysql
from pymongo import MongoClient

# MySQL connection
def mysql_connection():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch',
        charset='utf8mb4')

# MongoDB connection
def mongodb_connection():
    client = MongoClient('mongodb', 27017)
    return client['tpch']

# Execute MySQL query
def query_mysql(connection):
    with connection.cursor() as cursor:
        query = """
        SELECT PS_PARTKEY, PS_SUPPKEY
        FROM partsupp
        """
        cursor.execute(query)
        return cursor.fetchall()

# Query MongoDB and filter data
def query_mongodb(db, partkeys, suppkeys_to_exclude):
    part_projection = {
        'P_PARTKEY': 1, 'P_BRAND': 1, 'P_TYPE': 1, 'P_SIZE': 1
    }
    part_query = {
        'P_PARTKEY': {'$in': partkeys},
        'P_BRAND': {'$ne': 'Brand#45'},
        'P_TYPE': {'$not': {'$regex': '^MEDIUM POLISHED'}},
        'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]}
    }
    parts = db.part.find(part_query, part_projection)
    
    supp_projection = {
        'S_SUPPKEY': 1
    }
    supp_query = {
        'S_SUPPKEY': {'$nin': suppkeys_to_exclude},
        'S_COMMENT': {'$not': {'$regex': 'Customer Complaints'}}
    }
    suppliers = db.supplier.find(supp_query, supp_projection)
    
    return list(parts), list(suppliers)

# Merge data from MySQL and MongoDB
def merge_data(mysql_data, mongodb_parts, mongodb_suppliers):
    supplier_part_map = {}
    for part in mongodb_parts:
        supplier_part_map[part['P_PARTKEY']] = part
        
    suppliers_set = set(supplier['S_SUPPKEY'] for supplier in mongodb_suppliers)
    
    merged_data = []
    for partkey, suppkey in mysql_data:
        if suppkey in suppliers_set and partkey in supplier_part_map:
            part = supplier_part_map[partkey]
            merged_data.append((part['P_BRAND'], part['P_TYPE'], part['P_SIZE'], suppkey))
    return merged_data

# Main execution
def main():
    mysql_conn = mysql_connection()
    mongodb_db = mongodb_connection()
    
    mysql_data = query_mysql(mysql_conn)
    partkeys = [row[0] for row in mysql_data]
    suppkeys_to_exclude = [row[1] for row in mysql_data if 'Customer Complaints' in row[4]]
    
    mongodb_parts, mongodb_suppliers = query_mongodb(mongodb_db, partkeys, suppkeys_to_exclude)
    
    merged_data = merge_data(mysql_data, mongodb_parts, mongodb_suppliers)
    
    grouped_results = {}
    for brand, ptype, size, suppkey in merged_data:
        key = (brand, ptype, size)
        if key not in grouped_results:
            grouped_results[key] = set()
        grouped_results[key].add(suppkey)
    
    results = [(brand, ptype, size, len(suppliers)) for (brand, ptype, size), suppliers in grouped_results.items()]
    results.sort(key=lambda x: (-x[3], x[0], x[1], x[2]))
    
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])
        csvwriter.writerows(results)
        
    mysql_conn.close()

if __name__ == '__main__':
    main()
