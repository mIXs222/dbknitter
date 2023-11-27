from pymongo import MongoClient
import pandas as pd
import direct_redis

# Mongodb connection
client = MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]
nation = pd.DataFrame(list(mongodb["nation"].find({}, {'_id': 0})))

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
customer = pd.read_json(redis_client.get('customer'))
orders = pd.read_json(redis_client.get('orders'))
lineitem = pd.read_json(redis_client.get('lineitem'))

# Merge dataframes based on conditions
merged_df = (
    customer.merge(orders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
    .merge(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
    .merge(nation, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
)

# Apply filters
filtered_df = merged_df.query(
    "(O_ORDERDATE >= '1993-10-01') & "
    "(O_ORDERDATE < '1994-01-01') & "
    "(L_RETURNFLAG == 'R')"
)

# Perform calculations and grouping
result_df = (
    filtered_df.assign(REVENUE=lambda x: x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']))
    .groupby(
        [
            "C_CUSTKEY", "C_NAME", "C_ACCTBAL", "C_PHONE", "N_NAME", "C_ADDRESS", "C_COMMENT"
        ],
        as_index=False
    )
    .agg({'REVENUE': 'sum'})
)

# Sort as specified in the SQL order
result_df = result_df.sort_values(by=["REVENUE", "C_CUSTKEY", "C_NAME", "C_ACCTBAL"], ascending=[False, True, True, False])

# Write the output to a CSV file
result_df.to_csv('query_output.csv', index=False)
