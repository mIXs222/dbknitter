from pymongo import MongoClient
import csv
import datetime

def mongo_query(mongo_host, mongo_port, mongo_db):
    client = MongoClient(mongo_host, mongo_port)
    db = client[mongo_db]
    
    pipeline = [
        {
            "$lookup": {
                "from": "supplier",
                "localField": "L_SUPPKEY",
                "foreignField": "S_SUPPKEY",
                "as": "supplier_docs"
            }
        },
        {
            "$lookup": {
                "from": "orders",
                "localField": "L_ORDERKEY",
                "foreignField": "O_ORDERKEY",
                "as": "order_docs"
            }
        },
        {
            "$lookup": {
                "from": "customer",
                "localField": "O_CUSTKEY",
                "foreignField": "C_CUSTKEY",
                "as": "customer_docs"
            }
        },
        {
            "$lookup": {
                "from": "nation",
                "localField": "S_NATIONKEY",
                "foreignField": "N_NATIONKEY",
                "as": "nation_docs"
            }
        },
        {
            "$project": {
                "SUPP_NATION": "$nation_docs.N_NAME",
                "CUST_NATION": "$customer_docs.C_NATION",
                "L_YEAR": { "$year": "$L_SHIPDATE" },
                "L_EXTENDEDPRICE": 1,
                "L_DISCOUNT": 1
            }
        },
        {
            "$match":...
            ...To be completed by you...
        },
        {
            "$group": ...
            ...To be completed by you...
        },
        {
            "$sort": ...
            ...To be completed by you...
        }
    ]

    data = list(db['lineitem'].aggregate(pipeline))
    return data


def write_to_csv(query_result):
    with open('query_output.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['SUPP_NATION', 'CUST_NATION', 'L_YEAR', 'REVENUE'])
        for row in query_result:
            writer.writerow([row['SUPP_NATION'], row['CUST_NATION'], row['L_YEAR'], row['REVENUE']])


if __name__ == "__main__":
    query_result = mongo_query('mongodb', 27017, 'tpch') # Connect to MongoDB and execute query
    write_to_csv(query_result) # Write results to CSV
