import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Function to connect to MySQL and get the relevant part data
def get_mysql_data():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch',
                                 cursorclass=pymysql.cursors.Cursor)  # not using dict cursor as per instruction
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT P_PARTKEY, P_BRAND, P_CONTAINER
                FROM part
                WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG';
            """
            cursor.execute(query)
            result = cursor.fetchall()
            part_df = pd.DataFrame(result, columns=['P_PARTKEY', 'P_BRAND', 'P_CONTAINER'])
            return part_df
    finally:
        connection.close()

# Function to connect to Redis and get the relevant lineitem data
def get_redis_data():
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')
    return lineitem_df

def main():
    # Get data from mysql and redis
    part_df = get_mysql_data()
    lineitem_df = get_redis_data()

    # Merge MySQL and Redis data
    merged_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

    # Calculate the average quantity for the specified parts
    average_quantity = merged_df['L_QUANTITY'].mean()

    # Select only orders with quantity less than 20% of the average quantity
    small_quantity_df = merged_df[merged_df['L_QUANTITY'] < (0.20 * average_quantity)]

    # Calculate the average yearly loss in revenue
    small_quantity_df['LOSS_REVENUE'] = small_quantity_df['L_EXTENDEDPRICE']
    avg_yearly_revenue_loss = small_quantity_df['LOSS_REVENUE'].sum() / 7

    # Save results to CSV
    avg_yearly_revenue_loss_df = pd.DataFrame({'AverageYearlyRevenueLoss': [avg_yearly_revenue_loss]})
    avg_yearly_revenue_loss_df.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
