import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query execution
def get_mysql_data():
    connection = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        database='tpch',
    )
    try:
        with connection.cursor() as cursor:
            query = """
            SELECT o.O_ORDERDATE, l.L_PARTKEY, l.L_SUPPKEY, l.L_QUANTITY, l.L_EXTENDEDPRICE, l.L_DISCOUNT
            FROM orders o
            JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
            """
            cursor.execute(query)
            mysql_data = cursor.fetchall()
    finally:
        connection.close()
    return mysql_data

# MongoDB connection and query execution
def get_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    
    nations = list(db.nation.find({}, {'_id': 0}))
    parts = list(db.part.find({'P_NAME': {'$regex': '.*dim.*', '$options': 'i'}}, {'_id': 0}))

    client.close()
    return nations, parts

# Redis connection and query execution
def get_redis_data():
    client = DirectRedis(host='redis', port=6379, db=0)

    supplier_data = pd.read_json(client.get('supplier'), orient='records')
    partsupp_data = pd.read_json(client.get('partsupp'), orient='records')

    return supplier_data, partsupp_data

# Data transformation and combination
def process_data(mysql_data, nations, parts, supplier_data, partsupp_data):
    order_df = pd.DataFrame(mysql_data, columns=['O_ORDERDATE', 'L_PARTKEY', 'L_SUPPKEY', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])
    order_df['YEAR'] = pd.to_datetime(order_df['O_ORDERDATE']).dt.year
    
    nation_df = pd.DataFrame(nations)
    part_df = pd.DataFrame(parts)
    relevant_parts_df = part_df[part_df['P_NAME'].str.contains('dim', case=False, na=False)]
    
    part_keys = relevant_parts_df['P_PARTKEY'].unique().tolist()
    relevant_order_df = order_df[order_df['L_PARTKEY'].isin(part_keys)]
    
    supplier_df = supplier_data
    partsupp_df = partsupp_data

    # Merge dataframes
    merged_df = pd.merge(relevant_order_df, supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    merged_df = pd.merge(merged_df, partsupp_df, left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
    
    # Calculate profit
    merged_df['PROFIT'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])) - (merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY'])

    # Aggregate profit by NATION and YEAR
    profit_df = (
        merged_df.groupby(['S_NATIONKEY', 'YEAR'])
        .agg({'PROFIT': 'sum'})
        .reset_index()
    )

    # Merge with nation names
    result_df = pd.merge(profit_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    result_df.rename(columns={'N_NAME': 'NATION'}, inplace=True)
    
    # Final output with sorted nation and year
    result_df = result_df.sort_values(by=['NATION', 'YEAR'], ascending=[True, False])
    final_output = result_df[['NATION', 'YEAR', 'PROFIT']]
    
    return final_output

# Main function to perform query and output to CSV
def main():
    mysql_data = get_mysql_data()
    nations, parts = get_mongodb_data()
    supplier_data, partsupp_data = get_redis_data()
    
    final_output = process_data(mysql_data, nations, parts, supplier_data, partsupp_data)
    final_output.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
