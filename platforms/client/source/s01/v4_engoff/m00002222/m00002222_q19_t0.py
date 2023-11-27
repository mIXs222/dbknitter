# query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Function to connect to MySQL database and fetch relevant part data
def fetch_mysql_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    try:
        with connection.cursor() as cursor:
            sql = """SELECT P_PARTKEY, P_BRAND, P_TYPE, P_CONTAINER, P_SIZE
                     FROM part
                     WHERE (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5)
                        OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10)
                        OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15);"""
            cursor.execute(sql)
            result = pd.DataFrame(cursor.fetchall(), columns=['P_PARTKEY', 'P_BRAND', 'P_TYPE', 'P_CONTAINER', 'P_SIZE'])
            return result
    finally:
        connection.close()

# Function to connect to Redis and fetch relevant lineitem data
def fetch_redis_data():
    direct_redis_client = DirectRedis(host='redis', port=6379, db=0)
    df_lineitem = pd.read_json(direct_redis_client.get('lineitem') or '[]')
    return df_lineitem

# Fetch data from MySQL and Redis
df_part = fetch_mysql_data()
df_lineitem = fetch_redis_data()

# Merge two DataFrames on P_PARTKEY and filter as per the conditions
result = pd.merge(df_lineitem, df_part, left_on='L_PARTKEY', right_on='P_PARTKEY')
result['DISCOUNT_PRICE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Define the conditions for each brand and container type
conditions = (
    ((result['P_BRAND'] == 'Brand#12') & (result['L_QUANTITY'] >= 1) & (result['L_QUANTITY'] <= 11)) |
    ((result['P_BRAND'] == 'Brand#23') & (result['L_QUANTITY'] >= 10) & (result['L_QUANTITY'] <= 20)) |
    ((result['P_BRAND'] == 'Brand#34') & (result['L_QUANTITY'] >= 20) & (result['L_QUANTITY'] <= 30))
)

# Apply the filter
result = result[conditions & (result['L_SHIPMODE'].isin(['AIR', 'AIR REG']))]

# Calculate gross discounted revenue
gross_discounted_revenue = result['DISCOUNT_PRICE'].sum()

# Write the result to a CSV file
result.to_csv('query_output.csv', index=False)

# Print out the gross discounted revenue
print("Gross Discounted Revenue: ", gross_discounted_revenue)
