import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection and query execution
def get_mysql_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    with connection.cursor() as cursor:
        query = """
        SELECT ps.PS_SUPPKEY, SUM(ps.PS_AVAILQTY) as total_availability 
        FROM partsupp ps
        JOIN part p ON ps.PS_PARTKEY = p.P_PARTKEY
        WHERE p.P_NAME LIKE '%forest%'
        GROUP BY ps.PS_SUPPKEY
        """
        cursor.execute(query)
        mysql_data = cursor.fetchall()
    connection.close()
    return {row[0]: row[1] for row in mysql_data}

# MongoDB connection and query execution
def get_mongodb_data():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client['tpch']
    pipeline = [
        {
            '$match': {
                'L_SHIPDATE': {
                    '$gte': datetime(1994, 1, 1),
                    '$lt': datetime(1995, 1, 1)
                }
            },
        },
        {
            '$group': {
                '_id': '$L_SUPPKEY',
                'total_shipped': {'$sum': '$L_QUANTITY'}
            }
        },
        {
            '$lookup': {
                'from': 'supplier',
                'localField': '_id',
                'foreignField': 'S_SUPPKEY',
                'as': 'supplier_info'
            }
        },
        {
            '$match': {
                'supplier_info.S_NATIONKEY': {
                    '$in': [country_code]
                }
            }
        }
    ]
    nation_cursor = db.nation.find({'N_NAME': 'CANADA'}, {'N_NATIONKEY': 1})
    country_code = [doc['N_NATIONKEY'] for doc in nation_cursor]
    
    lineitem_cursor = db.lineitem.aggregate(pipeline)
    mongodb_data = list(lineitem_cursor)
    
    client.close()
    
    filter_mongodb_data = {doc['_id']: doc['total_shipped'] for doc in mongodb_data if 'supplier_info' in doc and doc['supplier_info']}
    return filter_mongodb_data

# Process and combine data from both DBMS
def process_data(mysql_data, mongodb_data):
    suppliers_with_excess = []
    for suppkey, supply_qty in mysql_data.items():
        if suppkey in mongodb_data and supply_qty > (mongodb_data[suppkey] * 0.5):
            suppliers_with_excess.append([suppkey, supply_qty])

    return suppliers_with_excess

# Write results to CSV file
def write_results_to_csv(data):
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['S_SUPPKEY', 'TOTAL_AVAILABILITY'])
        for row in data:
            writer.writerow(row)

def main():
    mysql_data = get_mysql_data()
    mongodb_data = get_mongodb_data()
    combined_data = process_data(mysql_data, mongodb_data)
    write_results_to_csv(combined_data)

if __name__ == "__main__":
    main()
