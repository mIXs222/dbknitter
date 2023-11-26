# import required libraries
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Function to execute query in MySQL
def get_mysql_data():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch')
    cursor = connection.cursor()
    query = """
    SELECT
        O_ORDERKEY,
        O_CUSTKEY,
        O_ORDERDATE,
        S_SUPPKEY,
        S_NATIONKEY,
        N_NATIONKEY,
        N_NAME,
        N_REGIONKEY
    FROM
        orders
        JOIN supplier ON O_ORDERKEY = supplier.S_SUPPKEY
        JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY
    WHERE
        O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
    """
    cursor.execute(query)
    result_set = cursor.fetchall()
    mysql_df = pd.DataFrame(result_set, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE', 'S_SUPPKEY', 'S_NATIONKEY', 'N_NATIONKEY', 'N_NAME', 'N_REGIONKEY'])
    cursor.close()
    connection.close()
    return mysql_df

# Function to execute query in MongoDB
def get_mongo_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    part_df = pd.DataFrame(list(db.part.find({'P_TYPE': 'SMALL PLATED COPPER'})))
    customer_df = pd.DataFrame(list(db.customer.find({})))
    client.close()
    return part_df, customer_df

# Function to fetch data from Redis
def get_redis_data():
    client = DirectRedis(host='redis', port=6379, db=0)
    region_df = pd.read_json(client.get('region'), orient='index')
    lineitem_df = pd.read_json(client.get('lineitem'), orient='index')
    return region_df, lineitem_df

# Fetching the data
mysql_data = get_mysql_data()
part_df, customer_df = get_mongo_data()
region_df, lineitem_df = get_redis_data()

# Merging the data from different sources
merged_df = pd.merge(part_df, lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')
merged_df = pd.merge(merged_df, mysql_data, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = pd.merge(merged_df, customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = pd.merge(merged_df, region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filter ASIA region and compile the volume
merged_df['O_YEAR'] = merged_df['O_ORDERDATE'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').year)
merged_df['VOLUME'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
merged_df = merged_df[merged_df['R_NAME'] == 'ASIA']

# Execute the analytic query
result_df = (merged_df.groupby('O_YEAR')
                    .apply(lambda x: pd.Series({
                        'MKT_SHARE': (x[x['N_NAME'] == 'INDIA']['VOLUME'].sum() / x['VOLUME'].sum())
                    }))
                    .reset_index())

# Write output to CSV
result_df.to_csv('query_output.csv', index=False)
