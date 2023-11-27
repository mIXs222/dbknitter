import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query execution
def get_parts_from_mysql():
    conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw',
                           database='tpch', cursorclass=pymysql.cursors.Cursor)
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT P_PARTKEY, P_MFGR
                FROM part
                WHERE P_TYPE = 'BRASS' AND P_SIZE = 15
            """
            cursor.execute(sql)
            result = cursor.fetchall()
            part_df = pd.DataFrame(result, columns=['P_PARTKEY', 'P_MFGR'])
    finally:
        conn.close()
    return part_df

# MongoDB connection and query execution
def get_region_and_partsupp_from_mongodb():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    region_collection = db['region']
    partsupp_collection = db['partsupp']
    # Get the region key for EUROPE
    region_key = region_collection.find_one({'R_NAME': 'EUROPE'})['R_REGIONKEY']
    # Get partsupp entries for parts with key obtained from MySQL part query
    partsupp_cursor = partsupp_collection.find({'PS_SUPPLYCOST': {'$gt': 0}})
    partsupp_df = pd.DataFrame(list(partsupp_cursor))
    client.close()
    return region_key, partsupp_df

# Redis connection and query execution
def get_nation_and_supplier_from_redis():
    r = DirectRedis(host='redis', port=6379, db=0)
    nation_df = pd.read_json(r.get('nation'))
    supplier_df = pd.read_json(r.get('supplier'))
    return nation_df, supplier_df

# Main execution function
def main():
    # Get data from MySQL
    parts_df = get_parts_from_mysql()

    # Get data from MongoDB
    region_key, partsupp_df = get_region_and_partsupp_from_mongodb()

    # Get data from Redis
    nation_df, supplier_df = get_nation_and_supplier_from_redis()

    # Merge and process data in Pandas
    # Select nations in EUROPE region
    nations_in_europe = nation_df[nation_df['N_REGIONKEY'] == region_key]

    # Merge to get suppliers in EUROPE region with nation details
    suppliers_in_europe = supplier_df.merge(nations_in_europe,
                                            left_on='S_NATIONKEY',
                                            right_on='N_NATIONKEY')

    # Merge partsupp with suppliers on supplier key
    partsupp_suppliers = partsupp_df.merge(suppliers_in_europe,
                                           left_on='PS_SUPPKEY',
                                           right_on='S_SUPPKEY')

    # Merge with part details on part key
    final_df = partsupp_suppliers.merge(parts_df, left_on='PS_PARTKEY',
                                        right_on='P_PARTKEY')

    # Sort and drop duplicates keeping the row with minimum PS_SUPPLYCOST
    final_sorted = final_df.sort_values(by=['PS_SUPPLYCOST', 'S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'],
                                        ascending=[True, False, True, True, True])
    final_sorted = final_sorted.drop_duplicates(subset=['P_PARTKEY'], keep='first')

    # Select the required columns and save to CSV
    final_sorted = final_sorted[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR',
                                 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]
    final_sorted.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
