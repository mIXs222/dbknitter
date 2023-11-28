import pymysql
import pymongo
import csv

def connect_mysql(host, user, password, db):
    return pymysql.connect(host=host, user=user, password=password, db=db)

def connect_mongodb(host, port, db):
    client = pymongo.MongoClient(host, port)
    return client[db]

def main():
    # Connect to MySQL
    mysql_conn = connect_mysql(host='mysql', user='root', password='my-secret-pw', db='tpch')
    mysql_cursor = mysql_conn.cursor()

    # Connect to MongoDB
    mongodb = connect_mongodb(host='mongodb', port=27017, db='tpch')
    
    # MySQL query
    mysql_query = """
    SELECT s.S_ACCTBAL, s.S_NAME, s.S_ADDRESS, s.S_PHONE, s.S_COMMENT, p.P_PARTKEY, p.P_MFGR, p.P_SIZE
    FROM supplier AS s
    JOIN nation AS n ON s.S_NATIONKEY = n.N_NATIONKEY
    JOIN region AS r ON n.N_REGIONKEY = r.R_REGIONKEY
    JOIN part AS p ON p.P_SIZE = 15 AND p.P_TYPE LIKE '%BRASS'
    WHERE r.R_NAME = 'EUROPE'
    ORDER BY s.S_ACCTBAL DESC, n.N_NAME, s.S_NAME, p.P_PARTKEY
    """
    mysql_cursor.execute(mysql_query)
    mysql_data = mysql_cursor.fetchall()

    # MongoDB query
    mongodb_data = list(mongodb.partsupp.find({"PS_SUPPLYCOST": {"$gt": 0}}, {"_id": 0}))

    # Combine data from MySQL and MongoDB
    combined_data = []
    for row in mysql_data:
        min_supply_cost = float('inf')
        min_supply_cost_comment = None
        
        for ps in mongodb_data:
            if ps['PS_PARTKEY'] == row[5] and ps['PS_SUPPKEY'] == int(row[0]):
                if ps['PS_SUPPLYCOST'] < min_supply_cost:
                    min_supply_cost = ps['PS_SUPPLYCOST']
                    min_supply_cost_comment = ps['PS_COMMENT']
        
        if min_supply_cost < float('inf'):
            combined_row = row + (min_supply_cost, min_supply_cost_comment)
            combined_data.append(combined_row)

    # Write output to file
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_PARTKEY', 'P_MFGR', 'P_SIZE', 'PS_SUPPLYCOST', 'PS_COMMENT'])
        writer.writerows(combined_data)
    
    # Close connections
    mysql_cursor.close()
    mysql_conn.close()

if __name__ == "__main__":
    main()
