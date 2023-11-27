import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MySQL query to get necessary data from the lineitem table
lineitem_query = """SELECT L_PARTKEY, L_QUANTITY, L_EXTENDEDPRICE 
                    FROM lineitem
                """
lineitem_df = pd.read_sql(lineitem_query, mysql_connection)
mysql_connection.close()

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Getting part data from Redis and transform to Dataframe
part_data = redis_connection.get('part')
part_df = pd.read_json(part_data)

# Filter to get parts of brand 'Brand#23' with 'MED BAG'
filtered_parts = part_df.loc[(part_df['P_BRAND'] == 'Brand#23') & (part_df['P_CONTAINER'] == 'MED BAG')]

# Join the two datasets on part key
merged_df = pd.merge(filtered_parts, lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Calculate the average quantity of orders
average_quantity = merged_df['L_QUANTITY'].mean()

# Calculate the threshold for the small quantity
small_quantity_threshold = 0.20 * average_quantity

# Find orders with small quantities
small_quantity_orders = merged_df[merged_df['L_QUANTITY'] < small_quantity_threshold]

# Calculate the average yearly gross loss
average_yearly_loss = small_quantity_orders['L_EXTENDEDPRICE'].sum() / 7

# Write output to a csv file
output = pd.DataFrame(data={'Average_Yearly_Loss': [average_yearly_loss]})
output.to_csv('query_output.csv', index=False)
