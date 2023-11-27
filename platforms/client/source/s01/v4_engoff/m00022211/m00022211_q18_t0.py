import pymongo
import pandas as pd
import redis
import direct_redis
import csv

# MongoDB Connection and Query
def mongodb_query():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client['tpch']
    
    pipeline = [
        {"$lookup": {
            "from": "orders",
            "localField": "L_ORDERKEY",
            "foreignField": "O_ORDERKEY",
            "as": "orders_data"
        }},
        {"$unwind": "$orders_data"},
        {"$group": {
            "_id": {
                "O_ORDERKEY": "$orders_data.O_ORDERKEY",
                "O_CUSTKEY": "$orders_data.O_CUSTKEY",
                "O_ORDERDATE": "$orders_data.O_ORDERDATE",
                "O_TOTALPRICE": "$orders_data.O_TOTALPRICE",
            },
            "total_quantity": {"$sum": "$L_QUANTITY"}
        }},
        {"$match": {
            "total_quantity": {"$gt": 300}
        }}
    ]
    
    results = list(db['lineitem'].aggregate(pipeline))
    client.close()
    
    return results

# Redis Connection and Query
def redis_query():
    client = direct_redis.DirectRedis(host='redis', port=6379)
    dataframe = client.get('customer')
    client.close()
    
    return dataframe

# Combine queries from different databases
def combine_query_results(mongodb_results, redis_df):
    combined_results = []
    for result in mongodb_results:
        customer_key = result["_id"]["O_CUSTKEY"]
        customer_details = redis_df.loc[redis_df['C_CUSTKEY'] == customer_key]
        
        for _, customer_row in customer_details.iterrows():
            combined_results.append({
                "C_NAME": customer_row["C_NAME"],
                "C_CUSTKEY": customer_key,
                "O_ORDERKEY": result["_id"]["O_ORDERKEY"],
                "O_ORDERDATE": result["_id"]["O_ORDERDATE"],
                "O_TOTALPRICE": result["_id"]["O_TOTALPRICE"],
                "total_quantity": result["total_quantity"],
            })
            
    return combined_results

# Perform queries and combine results
redis_df = pd.read_json(redis_query(), orient='records')
mongodb_results = mongodb_query()
final_results = combine_query_results(mongodb_results, redis_df)

# Write the query's output to a CSV file
keys = final_results[0].keys()
with open('query_output.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(final_results)
