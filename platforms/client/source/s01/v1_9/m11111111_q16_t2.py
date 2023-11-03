from pymongo import MongoClient
import pandas as pd
import re

client = MongoClient('mongodb://mongodb:27017/')
db = client["tpch"]

def query_mongodb():
    pipeline = [
        {"$lookup": 
            {"from": 'partsupp', 
            "localField": 'P_PARTKEY', 
            'foreignField': 'PS_PARTKEY', 
            'as': 'partsupps'}
        },
        {"$unwind": "$partsupps"},
        {"$match":
            {"$and": [
                {"P_BRAND": {"$ne": 'Brand#45'}},
                {"P_TYPE": {"$not": {"$regex": re.compile('^MEDIUM POLISHED.*')}}},
                {"P_SIZE": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]}},
                {"partsupps.PS_SUPPKEY": {"$nin": query_excluded_keys()}}
            ]}
        },
        {"$group": 
            {"_id": {"P_BRAND": '$P_BRAND', 'P_TYPE': '$P_TYPE', 'P_SIZE': '$P_SIZE'},
            'SUPPLIER_CNT': {"$sum": 1}}
        },
        {"$project": 
            { 'P_BRAND': '$_id.P_BRAND', 'P_TYPE': '$_id.P_TYPE', 'P_SIZE': '$_id.P_SIZE', 'SUPPLIER_CNT': 1, '_id': 0 }
        },
        {"$sort": 
            {"SUPPLIER_CNT": -1, 'P_BRAND': 1, 'P_TYPE': 1, 'P_SIZE': 1}
        }
    ]
    return list(db.part.aggregate(pipeline))

def query_excluded_keys():
    result = db.supplier.find(
        {"S_COMMENT": {"$regex": re.compile('.*Customer.*Complaints.*')}},
        {"S_SUPPKEY": 1, "_id": 0}
    )
    return [x['S_SUPPKEY'] for x in result]

def write_to_file():
    data = query_mongodb()
    df = pd.DataFrame(data)
    df.to_csv('query_output.csv', index=False)


if __name__ == '__main__':
    write_to_file()
