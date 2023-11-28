import csv
import pymysql
import pymongo

# Function to connect to MySQL
def connect_mysql(hostname, username, password, db_name):
    connection = pymysql.connect(host=hostname, user=username, password=password, db=db_name)
    return connection

# Function to connect to MongoDB
def connect_mongodb(hostname, port, db_name):
    client = pymongo.MongoClient(hostname, port)
    db = client[db_name]
    return db

# Execute query on MySQL
def query_mysql(connection):
    with connection.cursor() as cursor:
        query = """
        SELECT N_NATIONKEY, N_NAME
        FROM nation
        WHERE N_NAME = 'GERMANY'
        """
        cursor.execute(query)
        german_nation = cursor.fetchone()
        return german_nation if german_nation else (None, None)

# Execute query on MongoDB
def query_mongodb(db, german_nationkey):
    supplier_filter = {'S_NATIONKEY': german_nationkey}
    suppliers = list(db.supplier.find(supplier_filter, {'S_SUPPKEY': 1}))

    if not suppliers:
        return []

    supplier_ids = [s['S_SUPPKEY'] for s in suppliers]

    partsupp_filter = {'PS_SUPPKEY': {'$in': supplier_ids}}
    partsupps = list(db.partsupp.find(partsupp_filter))

    # Post-process to calculate total value for each part
    part_values = {}
    for ps in partsupps:
        value = ps['PS_SUPPLYCOST'] * ps['PS_AVAILQTY']
        part_key = ps['PS_PARTKEY']
        part_values[part_key] = part_values.get(part_key, 0) + value

    # Filter parts by value
    filtered_parts = [(k, v) for k, v in part_values.items() if v > 1000]  # Example threshold
    filtered_parts.sort(key=lambda x: x[1], reverse=True)

    return filtered_parts

def main():
    # Connect to MySQL
    mysql_conn = connect_mysql('mysql', 'root', 'my-secret-pw', 'tpch')
    
    # Get nation data from MySQL
    german_nationkey, german_name = query_mysql(mysql_conn)
    
    if not german_name:
        print("No German nation found in MySQL.")
        return
    
    # Connect to MongoDB
    mongodb_db = connect_mongodb('mongodb', 27017, 'tpch')
    
    # If the German nation is retrieved successfully, query MongoDB
    if german_nationkey is not None:
        parts_with_values = query_mongodb(mongodb_db, german_nationkey)
        
        # Write the query output to a CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['PS_PARTKEY', 'Total_Value'])  # Header
            for part_key, value in parts_with_values:
                writer.writerow([part_key, value])
    else:
        print("German nation not found, no data to export.")
    
    # Close MySQL connection
    mysql_conn.close()

if __name__ == '__main__':
    main()
