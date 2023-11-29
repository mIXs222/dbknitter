# Import required libraries
import pymysql
import pymongo
import pandas as pd
import direct_redis
from pandas import DataFrame
import csv
from datetime import datetime

# Function to get MySQL data
def get_mysql_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT
                    O_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT
                FROM
                    orders, lineitem
                WHERE
                    O_ORDERKEY = L_ORDERKEY AND
                    L_RETURNFLAG = 'R' AND
                    O_ORDERDATE BETWEEN '1993-10-01' AND '1994-01-01'
            """
            cursor.execute(query)
            result = cursor.fetchall()
            # Converting result to DataFrame
            mysql_df = DataFrame(list(result), columns=['O_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])
            return mysql_df
    finally:
        connection.close()

# Function to get MongoDB data
def get_mongodb_data():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client['tpch']
    nation_collection = db['nation']
    nations = list(nation_collection.find({}, {'_id':0, 'N_NATIONKEY':1, 'N_NAME':1}))
    # Converting result to DataFrame
    mongodb_df = DataFrame(nations)
    return mongodb_df

# Function to get Redis data
def get_redis_data():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    customer_data = r.get('customer')
    if customer_data is not None:
        # Assuming customer data is stored in JSON format in Redis
        customer_df = pd.read_json(customer_data, orient='records')
        return customer_df
    else:
        return DataFrame()

# Main function to process the data and output the result
def main():
    mysql_df = get_mysql_data()
    mongodb_df = get_mongodb_data()
    redis_df = get_redis_data()

    # Compute lost revenue for each order
    mysql_df['REVENUE_LOST'] = mysql_df.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']), axis=1)

    # Group by O_ORDERKEY to get total lost per customer
    revenuelost_per_order = mysql_df.groupby('O_ORDERKEY')['REVENUE_LOST'].sum().reset_index()

    # Merge with Redis customer data (assuming C_CUSTKEY corresponds to O_ORDERKEY)
    result_df = pd.merge(revenuelost_per_order, redis_df, how='inner', left_on='O_ORDERKEY', right_on='C_CUSTKEY')

    # Merge with MongoDB nation data
    result_final_df = pd.merge(result_df, mongodb_df, how='inner', left_on='C_NATIONKEY', right_on='N_NATIONKEY')

    # Select relevant columns and sort the data
    result_final_df = result_final_df[['C_CUSTKEY', 'C_NAME', 'REVENUE_LOST', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]
    result_final_df = result_final_df.sort_values(
        by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
        ascending=[True, True, True, False]
    )

    # Write to CSV file
    result_final_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
