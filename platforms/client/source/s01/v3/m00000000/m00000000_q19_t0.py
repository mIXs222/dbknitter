import csv
import mysql.connector
from pymongo import MongoClient

def execute_query(database, query):
    if database == 'mysql':
        user = 'root'
        password = 'my-secret-pw'
        host = 'mysql'
        
        conn = mysql.connector.connect(user=user, password=password, host=host, database='tpch')
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor

    elif database == 'mongodb':
        client = MongoClient()
        db = client['tpch']
        return db.command({'aggregate': 'lineitem', 'pipeline': query})

def write_to_csv(data, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)

def main():
    query = """
    SELECT SUM(L_EXTENDEDPRICE*(1 - L_DISCOUNT)) AS REVENUE
    FROM lineitem, part..........
    """
    cursor = execute_query('mysql', query)
    data = [(row['REVENUE'], ) for row in cursor]
    write_to_csv(data, 'query_output.csv')

if __name__ == "__main__":
    main()
