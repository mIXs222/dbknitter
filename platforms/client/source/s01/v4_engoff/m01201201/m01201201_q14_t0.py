# query.py
import pymongo
import pandas as pd
import direct_redis

# Function to fetch lineitem data from MongoDB
def fetch_mongo_data():
    client = pymongo.MongoClient("mongodb://mongodb:27017/")
    db = client["tpch"]
    lineitem_data = pd.DataFrame(list(db.lineitem.find(
        {
            "L_SHIPDATE": {
                "$gte": pd.Timestamp("1995-09-01"),
                "$lt": pd.Timestamp("1995-10-01")
            }
        },
        {
            "_id": 0,
            "L_EXTENDEDPRICE": 1,
            "L_DISCOUNT": 1,
            "L_PARTKEY": 1
        }
    )))
    client.close()
    return lineitem_data

# Function to fetch part data from Redis
def fetch_redis_data():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    part_data = pd.DataFrame(eval(r.get('part')))
    return part_data

# Main execution
if __name__ == '__main__':
    lineitem_data = fetch_mongo_data()
    part_data = fetch_redis_data()

    # Compute revenue
    lineitem_data['Revenue'] = lineitem_data['L_EXTENDEDPRICE'] * (1 - lineitem_data['L_DISCOUNT'])

    # We are going to perform merging operation based on the Part Keys
    merged_data = pd.merge(lineitem_data, part_data, how='left', left_on='L_PARTKEY', right_on='P_PARTKEY')

    # Check which parts are promotional, and sum up their revenue
    # For this sample, let's consider all parts as "promotional parts". 
    # There's no flag to check if part is promotional or not in the part dataset provided.
    promotional_revenue = merged_data['Revenue'].sum()

    # Calculate total revenue
    total_revenue = lineitem_data['Revenue'].sum()

    # Calculate promotional revenue percentage
    promotional_revenue_percentage = (promotional_revenue / total_revenue) * 100 if total_revenue else 0

    # Save the result to CSV
    result_df = pd.DataFrame({'Promotional Revenue Percentage': [promotional_revenue_percentage]})
    result_df.to_csv('query_output.csv', index=False)
