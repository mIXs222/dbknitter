import pymongo
import pandas as pd
from datetime import datetime
import direct_redis
from dateutil.parser import parse

# Define the range of the dates
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]
lineitem_collection = mongodb["lineitem"]

# Query MongoDB for line items shipped within the specified timeframe
lineitem_df = pd.DataFrame(list(lineitem_collection.find({
    "L_SHIPDATE": {
        "$gte": start_date,
        "$lte": end_date
    }
}, {
    "_id": 0,
    "L_SUPPKEY": 1,
    "L_EXTENDEDPRICE": 1,
    "L_DISCOUNT": 1
})))

# Calculate revenue for each supplier
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
revenue0 = lineitem_df.groupby("L_SUPPKEY", as_index=False).agg(TOTAL_REVENUE=('REVENUE', 'sum'))

# Connect to Redis and query for supplier table
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
supplier_data = redis_client.get('supplier')
supplier_df = pd.read_json(supplier_data.decode("utf-8"))

# Merge the CTE (revenue0) with supplier dataframe
result_df = pd.merge(supplier_df, revenue0, left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Determine the supplier with maximum revenue during the timeframe
max_revenue_supplier = result_df[result_df['TOTAL_REVENUE'] == result_df['TOTAL_REVENUE'].max()]

# Order the output by S_SUPPKEY and output to 'query_output.csv'
max_revenue_supplier.sort_values('S_SUPPKEY').to_csv('query_output.csv', index=False)
