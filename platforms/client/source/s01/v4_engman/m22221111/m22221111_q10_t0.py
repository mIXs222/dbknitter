# query.py
import pandas as pd
import pymongo
from bson.son import SON
import direct_redis

def fetch_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']

    pipeline = [
        {"$match": {
            "O_ORDERDATE": {
                "$gte": pd.Timestamp("1993-10-01"),
                "$lt": pd.Timestamp("1994-01-01"),
            },
            "L_RETURNFLAG": "R",
        }},
        {"$lookup": {
            "from": "customer",
            "localField": "O_CUSTKEY",
            "foreignField": "C_CUSTKEY",
            "as": "customer_data",
        }},
        {"$unwind": "$customer_data"},
        {"$project": {
            "C_CUSTKEY": "$customer_data.C_CUSTKEY",
            "C_NAME": "$customer_data.C_NAME",
            "C_ADDRESS": "$customer_data.C_ADDRESS",
            "C_PHONE": "$customer_data.C_PHONE",
            "C_ACCTBAL": "$customer_data.C_ACCTBAL",
            "C_COMMENT": "$customer_data.C_COMMENT",
            "C_NATIONKEY": "$customer_data.C_NATIONKEY",
            "revenue_lost": {
                "$multiply": [
                    {"$subtract": [
                        1,
                        "$L_DISCOUNT"
                    ]},
                    "$L_EXTENDEDPRICE"
                ]
            },
        }},
        {"$group": {
            "_id": {
                "C_CUSTKEY": "$C_CUSTKEY",
                "C_NAME": "$C_NAME",
                "C_ACCTBAL": "$C_ACCTBAL",
                "C_NATIONKEY": "$C_NATIONKEY",
                "C_ADDRESS": "$C_ADDRESS",
                "C_PHONE": "$C_PHONE",
                "C_COMMENT": "$C_COMMENT",
            },
            "total_revenue_lost": {"$sum": "$revenue_lost"},
        }},
        {"$sort": SON([
            ("total_revenue_lost", 1),
            ("_id.C_CUSTKEY", 1),
            ("_id.C_NAME", 1),
            ("_id.C_ACCTBAL", -1),
        ])}
    ]

    result = list(db.orders.aggregate(pipeline))
    mongo_df = pd.DataFrame(result)
    if not mongo_df.empty:
        mongo_df = pd.json_normalize(mongo_df['_id'])
        mongo_df['total_revenue_lost'] = pd.json_normalize(result)['total_revenue_lost']
    return mongo_df

def fetch_redis_data():
    dr = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    nation_df = pd.read_parquet(dr.get('nation'), engine='pyarrow')
    return nation_df

def merge_data(mongo_data, redis_data):
    merged_df = pd.merge(
        mongo_data,
        redis_data,
        how='left',
        left_on='C_NATIONKEY',
        right_on='N_NATIONKEY'
    )
    return merged_df[['C_CUSTKEY', 'C_NAME', 'total_revenue_lost', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]

def main():
    mongo_data = fetch_mongodb_data()
    redis_data = fetch_redis_data()
    final_data = merge_data(mongo_data, redis_data)
    final_data.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
