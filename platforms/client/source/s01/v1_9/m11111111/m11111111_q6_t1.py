from pymongo import MongoClient
import csv
from datetime import datetime

def execute_query():
    client = MongoClient("mongodb://mongodb:27017/")
    db = client["tpch"]

    lineitem = db["lineitem"]

    revenue = 0
    for doc in lineitem.find(
        {"L_SHIPDATE": { "$gte": "1994-01-01", "$lt": "1995-01-01"},
         "L_DISCOUNT": { "$gte": .06 - 0.01, "$lte": .06 + 0.01 },
         "L_QUANTITY": { "$lt": 24}}, 
        {"L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1}):
        
        revenue += doc["L_EXTENDEDPRICE"] * doc["L_DISCOUNT"]

    return revenue

def write_to_csv(revenue):
    with open('query_output.csv', mode='w') as file:
        writer = csv.writer(file)
        writer.writerow(["REVENUE"])
        writer.writerow([revenue])

def main():
    revenue = execute_query()
    write_to_csv(revenue)

if __name__ == "__main__":
    main()
