from pymongo import MongoClient
import pandas as pd

# Function to connect to MongoDB
def connect_mongo():
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['tpch']
    return db

# Function to execute condition check
def check_condition(p, l):
    conditions = [
        (
            p['P_BRAND'] == 'Brand#12'
            and p['P_CONTAINER'] in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l['L_QUANTITY'] >= 1 
            and l['L_QUANTITY'] <= 11 
            and p['P_SIZE'] between 1 and 5
        ),
        (
            p['P_BRAND'] == 'Brand#23'
            and p['P_CONTAINER'] in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l['L_QUANTITY'] >= 10 
            and l['L_QUANTITY'] <= 20 
            and p['P_SIZE'] between 1 and 10
        ),
        (
            p['P_BRAND'] == 'Brand#34'
            and p['P_CONTAINER'] in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l['L_QUANTITY'] >= 20 
            and l['L_QUANTITY'] <= 30 
            and p['P_SIZE'] between 1 and 15
        )
    ]
    return any(conditions)


def main():
    # Connect to MongoDB
    db = connect_mongo()

    results = []

    # Iterating through all documents in collections "part" and "lineitem"
    for p in db.part.find():
        for l in db.lineitem.find():
            # Check if partkey equals and conditions meet
            if p['P_PARTKEY'] == l['L_PARTKEY'] and l['L_SHIPMODE'] in ('AIR', 'AIR REG') and l['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON' and check_condition(p, l):
                results.append(l['L_EXTENDEDPRICE'] * (1 - l['L_DISCOUNT']))
    
    # Calculating sum
    sum_result = sum(results)
    
    # Writing result to csv file
    pd.DataFrame([sum_result], columns=["REVENUE"]).to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
