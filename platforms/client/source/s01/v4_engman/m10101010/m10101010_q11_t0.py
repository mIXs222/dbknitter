import pymysql
import pymongo
import csv

# Function to connect to MySQL
def connect_mysql(hostname, username, password, dbname):
    connection = pymysql.connect(host=hostname,
                                 user=username,
                                 password=password,
                                 database=dbname,
                                 cursorclass=pymysql.cursors.Cursor)
    return connection

# Function to connect to MongoDB
def connect_mongodb(hostname, database, port=27017):
    client = pymongo.MongoClient(host=hostname, port=port)
    return client[database]

# Execute query in MySQL
def get_suppliers_from_germany(connection):
    with connection.cursor() as cursor:
        query = """
        SELECT S_SUPPKEY, S_NAME
        FROM supplier
        WHERE S_NATIONKEY = (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY')
        """
        cursor.execute(query)
        return cursor.fetchall()

# Get part supplies from MongoDB
def get_partsupplies(mongo_db):
    partsupp_data = mongo_db.partsupp.find()
    return {(item['PS_SUPPKEY'], item['PS_PARTKEY']): item for item in partsupp_data}

# Merge data from both databases
def merge_data(suppliers, partsupplies):
    significant_parts = {}
    total_value = 0

    # Calculate totals
    for (suppkey, partkey), partsupp in partsupplies.items():
        total_value += partsupp['PS_AVAILQTY'] * partsupp['PS_SUPPLYCOST']

    # Find important stock
    for (suppkey, partkey), partsupp in partsupplies.items():
        if suppkey in suppliers:
            value = partsupp['PS_AVAILQTY'] * partsupp['PS_SUPPLYCOST']
            if value / total_value > 0.0001:
                significant_parts[partkey] = significant_parts.get(partkey, 0) + value

    return sorted(significant_parts.items(), key=lambda item: item[1], reverse=True)

# Main function to run the logic
def main():
    # Connect to databases
    mysql_conn = connect_mysql('mysql', 'root', 'my-secret-pw', 'tpch')
    mongo_db = connect_mongodb('mongodb', 'tpch')

    try:
        # Get data from both databases
        suppliers = {key: name for key, name in get_suppliers_from_germany(mysql_conn)}
        partsupplies = get_partsupplies(mongo_db)

        # Merge data and process
        significant_parts = merge_data(suppliers, partsupplies)

        # Write results to CSV
        with open('query_output.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['PARTKEY', 'VALUE'])

            for partkey, value in significant_parts:
                writer.writerow([partkey, value])
                
    finally:
        mysql_conn.close()

# Run the main function
if __name__ == "__main__":
    main()
