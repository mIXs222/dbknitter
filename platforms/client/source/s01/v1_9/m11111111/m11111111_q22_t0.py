from pymongo import MongoClient
import pandas as pd

def mongo_connect():
    try:
        client = MongoClient("mongodb://mongodb:27017/")
        print("MongoDB connected successfully!!!")
    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to MongoDB. Are you sure it is running? Error:", e)
    return client

def execute_query():
    client = mongo_connect()
    db = client['tpch']

    pipeline = [
            {
                "$match": {
                    "$and": [
                        {
                            "C_PHONE": {"$in": ["20", "40", "22", "30", "39", "42", "21"],
                            "C_ACCTBAL": {"$gt": 0}
                        }
                    ]
                }
            },
            {
                "$group": {
                    "_id": {"CNTRYCODE": {"$substr": ["$C_PHONE", 0, 2]}},
                    "NUMCUST": {"$sum": 1},
                    "TOTACCTBAL": {"$sum": "$C_ACCTBAL"}
                 }
            },
            {
                "$project": {
                    "CNTRYCODE": "$_id.CNTRYCODE",
                    "NUMCUST": 1,
                    "TOTACCTBAL": 1
                }
            },
            {
                "$sort": {"CNTRYCODE": 1}
            }
        ]
    results = db.customer.aggregate(pipeline)

    df = pd.DataFrame(list(results))

    # Write DataFrame to CSV
    df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    execute_query()
