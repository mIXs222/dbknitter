import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to get data from MongoDB
def get_mongo_data():
    client = pymongo.MongoClient(host='mongodb', port=27017)
    db = client['tpch']
    part_collection = db['part']
    query = {
        'P_BRAND': 'Brand#23',
        'P_CONTAINER': 'MED BAG'
    }
    return pd.DataFrame(list(part_collection.find(query, projection={'_id': False})))

# Function to get data from Redis
def get_redis_data():
    r = DirectRedis(host='redis', port=6379, db=0)
    lineitem_df = r.get('lineitem')
    return pd.read_csv(lineitem_df.decode('utf-8'))

# Main execution
def main():
    part_df = get_mongo_data()
    lineitem_df = get_redis_data()

    # Execute the subquery to calculate the average quantity for each part
    avg_quantity = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean() * 0.2
    avg_quantity = avg_quantity.reset_index().rename(columns={'L_QUANTITY': 'AVG_QUANTITY'})

    # Merge the lineitem and average quantity dataframes
    lineitem_df = lineitem_df.merge(avg_quantity, left_on='L_PARTKEY', right_on='L_PARTKEY')

    # Filter lineitem based on the quantity condition and merge with part dataframe
    lineitem_filtered = lineitem_df[lineitem_df['L_QUANTITY'] < lineitem_df['AVG_QUANTITY']]
    merged_df = lineitem_filtered.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

    # Compute the final result
    avg_yearly = merged_df['L_EXTENDEDPRICE'].sum() / 7.0

    # Write result to CSV
    pd.DataFrame({'AVG_YEARLY': [avg_yearly]}).to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
