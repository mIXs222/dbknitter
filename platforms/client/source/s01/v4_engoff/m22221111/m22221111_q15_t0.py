import pymongo
import pandas as pd
from direct_redis import DirectRedis

def main():
    # Connect to MongoDB
    mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
    mongo_db = mongo_client["tpch"]
    lineitem_collection = mongo_db["lineitem"]

    # Query for line items in the given date range
    lineitem_df = pd.DataFrame(list(lineitem_collection.find({
        "L_SHIPDATE": {"$gte": "1996-01-01", "$lt": "1996-04-01"}
    }, {
        "_id": 0,
        "L_SUPPKEY": 1,
        "L_EXTENDEDPRICE": 1,
        "L_DISCOUNT": 1
    })))

    # Calculate revenue contribution for each supplier in line items
    lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
    supplier_revenue = lineitem_df.groupby('L_SUPPKEY')['REVENUE'].sum().reset_index()

    # Connect to Redis
    redis_client = DirectRedis(host="redis", port=6379, db=0)

    # Fetch suppliers DataFrame
    suppliers_json = redis_client.get('supplier')
    if suppliers_json:
        suppliers_df = pd.read_json(suppliers_json)
        # Merge to find top suppliers by revenue
        merged_df = pd.merge(suppliers_df, supplier_revenue, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
        top_revenue = merged_df['REVENUE'].max()
        top_suppliers = merged_df[merged_df['REVENUE'] == top_revenue].sort_values(by='S_SUPPKEY')
        top_suppliers.to_csv('query_output.csv', index=False)
    else:
        print("Error: Unable to retrieve 'supplier' table from Redis")

    mongo_client.close()

if __name__ == "__main__":
    main()
