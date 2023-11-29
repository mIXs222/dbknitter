# query_exec.py
import pymongo
from bson import json_util
import pandas as pd
from datetime import datetime
import direct_redis


# Function to connect to MongoDB and retrieve the lineitem data
def get_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    lineitem_collection = db['lineitem']

    # Query to MongoDB to filter data as per given conditions
    query = {
        'L_SHIPDATE': {'$lt': 'L_COMMITDATE'},
        'L_RECEIPTDATE': {'$gte': '1994-01-01', '$lt': '1995-01-01'},
        'L_SHIPMODE': {'$in': ['mail', 'ship']},
        'L_COMMITDATE': {'$lt': 'L_RECEIPTDATE'},
        'L_LINESTATUS': 'F'
    }

    # Executing the query on mongodb
    lineitem_data = lineitem_collection.find(query, {'_id': 0}).sort('L_SHIPMODE', pymongo.ASCENDING)

    # Convert the mongodb cursor to json then to dataframe
    json_data = json_util.dumps(lineitem_data)
    df_lineitem = pd.read_json(json_data)

    return df_lineitem


# Function to connect to Redis and retrieve the orders data
def get_redis_data():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Retrieve orders data as json and convert to dataframe
    orders_json_data = r.get('orders')
    orders_data = json_util.loads(orders_json_data)
    df_orders = pd.DataFrame(orders_data)
    
    return df_orders


def main():
    # Get data from MongoDB and Redis
    df_lineitem = get_mongodb_data()
    df_orders = get_redis_data()

    # Merge the dataframes on the ORDERKEY columns
    merged_df = df_lineitem.merge(df_orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    
    # Filter based on priority and date conditions
    priority_conditions = {
        'High': (merged_df['O_ORDERPRIORITY'].isin(['URGENT', 'HIGH'])),
        'Low': (~merged_df['O_ORDERPRIORITY'].isin(['URGENT', 'HIGH']))
    }

    # Group and process data as per requirements
    def process_group(group):
        receipt_date_exceeds = group['L_RECEIPTDATE'] > group['L_COMMITDATE']
        return {
            'HighPriority_Count': group.loc[priority_conditions['High'] & receipt_date_exceeds].shape[0],
            'LowPriority_Count': group.loc[priority_conditions['Low'] & receipt_date_exceeds].shape[0]
        }

    grouped_data = merged_df.groupby('L_SHIPMODE').apply(process_group).reset_index()

    # Writing grouped and processed data to CSV file
    grouped_data.to_csv('query_output.csv', index=False)


if __name__ == '__main__':
    main()
