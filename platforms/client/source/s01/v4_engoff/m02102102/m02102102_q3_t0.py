import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to connect to MySQL database and retrieve the orders
def get_mysql_data():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch')
    try:
        with connection.cursor() as cursor:
            sql = """SELECT O_ORDERKEY, O_SHIPPRIORITY
                     FROM orders
                     WHERE O_ORDERDATE < '1995-03-15'
                       AND O_ORDERSTATUS = 'O'"""
            cursor.execute(sql)
            result = cursor.fetchall()
            orders_df = pd.DataFrame(list(result), columns=['O_ORDERKEY', 'O_SHIPPRIORITY'])
    finally:
        connection.close()

    return orders_df

# Function to connect to MongoDB database and retrieve the customers
def get_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    customer_collection = db['customer']
    
    customers_df = pd.DataFrame(list(customer_collection.find({"C_MKTSEGMENT": "BUILDING"})))
    return customers_df[['C_CUSTKEY']]

# Function to connect to Redis database and retrieve the lineitems
def get_redis_data():
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    lineitem_raw_data = redis_client.get('lineitem')
    lineitem_df = pd.read_json(lineitem_raw_data, orient='records')
    
    # Calculating potential revenue
    lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
    return lineitem_df[['L_ORDERKEY', 'REVENUE']]

# Main execution
def main():
    # Retrieve data from different databases
    orders_df = get_mysql_data()
    customers_df = get_mongodb_data()
    lineitem_df = get_redis_data()

    # Merge data on keys
    merged_df = orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
    merged_df = merged_df.merge(customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')

    # Group by order key and sum the revenues, then sort and select the top revenue order
    grouped_df = merged_df.groupby(['O_ORDERKEY', 'O_SHIPPRIORITY']).agg({'REVENUE': 'sum'}).reset_index()
    top_revenue_df = grouped_df.sort_values('REVENUE', ascending=False).head(1)

    # Write result to csv
    top_revenue_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
