import pymysql
import pandas as pd
import direct_redis

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Redis connection setup
redis_conn = direct_redis.DirectRedis(host="redis", port=6379, db=0)

# Fetch nation and partsupp data from MySQL
try:
    with mysql_conn.cursor() as cursor:
        # Fetch nation data where N_NAME is 'GERMANY'
        cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY'")
        nation_data = cursor.fetchall()
        
        # If GERMANY is found, proceed
        if nation_data:
            nationkey_germany = nation_data[0][0]
            
            # Fetch partsupp data
            cursor.execute("SELECT PS_PARTKEY, PS_AVAILQTY * PS_SUPPLYCOST AS VALUE FROM partsupp")
            partsupp_data = list(cursor.fetchall())
            
        else:
            partsupp_data = []

finally:
    mysql_conn.close()

# Fetch supplier data from Redis
supplier_data = pd.read_json(redis_conn.get('supplier'), orient='records')

# Filter suppliers from GERMANY
supplier_data_germany = supplier_data[supplier_data['S_NATIONKEY'] == nationkey_germany]

# Convert partsupp data to DataFrame
partsupp_df = pd.DataFrame(partsupp_data, columns=['PS_PARTKEY', 'VALUE'])

# Join the DataFrames and filtering
result = pd.merge(supplier_data_germany, partsupp_df, left_on='S_SUPPKEY', right_on='PS_PARTKEY')
result = result[result['VALUE'] / result['VALUE'].sum() > 0.0001]

# Select the necessary columns and sort
output = result[['PS_PARTKEY', 'VALUE']].sort_values(by=['VALUE'], ascending=False)

# Write to CSV
output.to_csv('query_output.csv', index=False)
