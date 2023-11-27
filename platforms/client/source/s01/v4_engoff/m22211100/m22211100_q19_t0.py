# discounted_revenue.py
import pymysql
import pandas as pd
import direct_redis

# Function to query MySQL database
def query_mysql():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch')
    try:
        with connection.cursor() as cursor:
            sql = """
            SELECT 
                l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) AS revenue,
                l.L_ORDERKEY,
                l.L_PARTKEY,
                l.L_QUANTITY,
                l.L_SHIPMODE
            FROM 
                lineitem l
            WHERE 
                l.L_SHIPMODE IN ('AIR', 'AIR REG') AND 
                l.L_SHIPINSTRUCT = 'DELIVER IN PERSON'
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            col_names = [i[0] for i in cursor.description]
            return pd.DataFrame(results, columns=col_names)
    finally:
        connection.close()

# Function to query Redis database
def query_redis():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    part_df = r.get('part')
    return part_df

# Combine both dataframes and filter as required
def filter_and_combine(mysql_df, redis_df):
    # Creating a mask for each of the three types in the redis_df
    mask_type1 = (redis_df.P_BRAND == 'Brand#12') & \
                 (redis_df.P_CONTAINER.isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & \
                 (redis_df.P_SIZE.between(1, 5))
    
    mask_type2 = (redis_df.P_BRAND == 'Brand#23') & \
                (redis_df.P_CONTAINER.isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & \
                (redis_df.P_SIZE.between(1, 10))

    mask_type3 = (redis_df.P_BRAND == 'Brand#34') & \
                (redis_df.P_CONTAINER.isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & \
                (redis_df.P_SIZE.between(1, 15))

    # Combine masks with OR
    combined_mask = mask_type1 | mask_type2 | mask_type3

    # Apply filter to redis_df and select relevant rows
    filtered_redis_df = redis_df[combined_mask]

    # Convert P_PARTKEY to int for merging
    filtered_redis_df['P_PARTKEY'] = filtered_redis_df['P_PARTKEY'].astype(int)

    # Merge MySQL and Redis dataframes on partkey
    merged_df = pd.merge(mysql_df, filtered_redis_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

    # Creating quantity masks for the merged_df
    quantity_mask_type1 = merged_df.L_QUANTITY.between(1, 11)
    quantity_mask_type2 = merged_df.L_QUANTITY.between(10, 20)
    quantity_mask_type3 = merged_df.L_QUANTITY.between(20, 30)

    # Combine quantity masks with corresponding type masks
    final_mask = (quantity_mask_type1 & mask_type1) | (quantity_mask_type2 & mask_type2) | (quantity_mask_type3 & mask_type3)
    
    # Select rows that match final mask and output columns
    final_df = merged_df[final_mask][['L_ORDERKEY', 'revenue']]

    return final_df

# Execute query functions and write output to CSV
def main():
    mysql_data = query_mysql()
    redis_data = query_redis()
    result = filter_and_combine(mysql_data, redis_data)
    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
