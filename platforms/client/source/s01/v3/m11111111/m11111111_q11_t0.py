from pymongo import MongoClient
import pandas as pd

def query_db():
    global result
    # create mongodb client
    mongodb_client = MongoClient("mongodb://mongodb:27017/")
    db = mongodb_client["tpch"]

    # implement SQL query in mongodb
    pipeline = [
        {"$match": {"N_NAME": "GERMANY"}},
        {"$lookup":{
            "from": "supplier",
            "localField": "N_NATIONKEY",
            "foreignField": "S_NATIONKEY",
            "as": "supplier_info"}},
        {"$lookup":{
            "from": "partsupp",
            "localField": "supplier_info.S_SUPPKEY",
            "foreignField": "PS_SUPPKEY",
            "as": "partsupp_info"}},
        {"$unwind":"$partsupp_info"},
        {"$unwind":"$supplier_info"},
        {"$group":{
            "_id":"$partsupp_info.PS_PARTKEY",
            "value": {"$sum": {
                "$multiply": [ "$partsupp_info.PS_SUPPLYCOST", "$partsupp_info.PS_AVAILQTY" ]}}}},
        {"$match": {"value": {"$gt": {
            "$sum": {
            "$multiply": [ "$partsupp_info.PS_SUPPLYCOST", "$partsupp_info.PS_AVAILQTY" ] * 0.0001000000}}}}},
        {"$sort": {"value": -1}}
    ]

    result = list(db.nation.aggregate(pipeline))

def save_to_csv():
    # converting the output to pandas dataframe
    result_df = pd.DataFrame(result)
    # output the result to csv
    result_df.to_csv("./query_output.csv", index=False)

if __name__ == "__main__":
    query_db()
    save_to_csv()
