import pandas as pd
import pymongo
from direct_redis import DirectRedis

def execute_mongodb_query():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    lineitem_collection = db['lineitem']
    lineitem_df = pd.DataFrame(list(lineitem_collection.find()))
    return lineitem_df

def execute_redis_query():
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    part_df = pd.read_json(redis_client.get('part'))
    return part_df

def calculate_revenue(lineitem_df, part_df):
    merge_conditions = [
        (part_df.P_BRAND == 'Brand#12') & part_df.P_CONTAINER.isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']),
        (part_df.P_BRAND == 'Brand#23') & part_df.P_CONTAINER.isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']),
        (part_df.P_BRAND == 'Brand#34') & part_df.P_CONTAINER.isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']),
    ]
    part_df_filtered = part_df[np.any(merge_conditions, axis=0)]

    joined_df = pd.merge(part_df_filtered, lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')
    joined_df['REVENUE'] = joined_df.L_EXTENDEDPRICE * (1 - joined_df.L_DISCOUNT)

    result = joined_df.query(
        "((P_BRAND == 'Brand#12' and L_QUANTITY >= 1 and L_QUANTITY <= 11 and P_SIZE between 1 and 5) or "
        "(P_BRAND == 'Brand#23' and L_QUANTITY >= 10 and L_QUANTITY <= 20 and P_SIZE between 1 and 10) or "
        "(P_BRAND == 'Brand#34' and L_QUANTITY >= 20 and L_QUANTITY <= 30 and P_SIZE between 1 and 15))"
        "and L_SHIPMODE in ['AIR', 'AIR REG'] and L_SHIPINSTRUCT == 'DELIVER IN PERSON'"
    )

    return result.groupby(by=lambda x: True)['REVENUE'].sum()

if __name__ == '__main__':
    lineitem_df = execute_mongodb_query()
    part_df = execute_redis_query()
    revenue = calculate_revenue(lineitem_df, part_df)
    revenue.to_csv('query_output.csv', header=True)
