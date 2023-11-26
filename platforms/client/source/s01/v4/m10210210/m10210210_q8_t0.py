import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query and load data from MySQL
mysql_query = """
SELECT
    l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) AS volume,
    o.O_ORDERDATE,
    l.L_ORDERKEY,
    l.L_PARTKEY,
    s.S_SUPPKEY,
    c.C_CUSTKEY,
    c.C_NATIONKEY,
    r.R_REGIONKEY,
    r.R_NAME
FROM
    lineitem l
JOIN
    orders o ON l.L_ORDERKEY = o.O_ORDERKEY
JOIN
    customer c ON o.O_CUSTKEY = c.C_CUSTKEY
JOIN
    region r ON c.C_NATIONKEY = r.R_REGIONKEY
WHERE
    r.R_NAME = 'ASIA'
    AND o.O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
"""
mysql_df = pd.read_sql_query(mysql_query, mysql_conn)

# Load data from MongoDB
nation_df = pd.DataFrame(list(mongo_db.nation.find()))
supplier_df = pd.DataFrame(list(mongo_db.supplier.find()))

# Load data from Redis
part_df = pd.read_msgpack(redis_conn.get('part'))

# Merge data frames
merged_df = mysql_df.merge(nation_df, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
merged_df = merged_df.merge(supplier_df, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
merged_df = merged_df.merge(part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER'], left_on="L_PARTKEY", right_on="P_PARTKEY")

# Preprocessing to perform the group by and sum
merged_df['O_YEAR'] = pd.to_datetime(merged_df['O_ORDERDATE']).dt.year
merged_df['NATION'] = merged_df['N_NAME']
merged_df['VOLUME'] = merged_df['volume']

# Execute the query logic on the merged dataframe
result_df = merged_df.groupby('O_YEAR').apply(
    lambda x: pd.Series({
        'MKT_SHARE': x[x['NATION'] == 'INDIA']['VOLUME'].sum() / x['VOLUME'].sum()
    })
).reset_index()

# Write the query output to a csv file
result_df.to_csv('query_output.csv', index=False)
