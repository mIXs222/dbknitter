import csv
from pymongo import MongoClient
import mysql.connector

def main():
    # MongoDB Connection
    client = MongoClient("mongodb://localhost:27017/")
    mongo_db = client["tpch"]
    lineitem = mongo_db["lineitem"]

    # MySQL Connection
    mysql_con = mysql.connector.connect(
        host="mysql",
        user="root",
        password="my-secret-pw",
        database="tpch"
    )

    # MySQL cursor
    mysql_cursor = mysql_con.cursor()

    # Query to MongoDB
    mongo_query = {
        "L_SHIPDATE": {"$gte": "1994-01-01", "$lt": "1995-01-01"}, 
        "L_DISCOUNT": {"$gte": .06 - 0.01, "$lte": .06 + 0.01},
        "L_QUANTITY": {"$lt": 24}
    }
    
    # Query to MySQL
    mysql_query = (
        "SELECT L_EXTENDEDPRICE, L_DISCOUNT FROM lineitem "
        "WHERE L_SHIPDATE >= '1994-01-01' "
        "AND L_SHIPDATE < '1995-01-01' "
        "AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01 "
        "AND L_QUANTITY < 24"
    )
                       
    mongo_results = lineitem.find(mongo_query)
    mysql_cursor.execute(mysql_query)
    mysql_results = mysql_cursor.fetchall()
    
    # Calculate revenue
    revenue = 0
    for result in mongo_results:
        revenue += result["L_EXTENDEDPRICE"] * result["L_DISCOUNT"]
    for (L_EXTENDEDPRICE, L_DISCOUNT) in mysql_results:
        revenue += L_EXTENDEDPRICE * L_DISCOUNT

    # Write the output
    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['REVENUE'])
        writer.writerow([revenue])

if __name__ == "__main__":
    main()
