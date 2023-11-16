import pymysql
import pymongo
import pandas as pd
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    port=3306,
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query MySQL
mysql_query = """
SELECT
    n.N_NAME,
    s.S_NATIONKEY,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    nation n JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
             JOIN supplier s ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE
    r.R_NAME = 'ASIA'
GROUP BY
    n.N_NAME, s.S_NATIONKEY
ORDER BY
    revenue DESC
"""

mysql_df = pd.read_sql(mysql_query, mysql_conn)

# Query MongoDB
match_stage = {
    '$match': {
        'O_ORDERDATE': {'$gte': '1990-01-01', '$lt': '1995-01-01'}
    }
}

lookup_customer = {
    '$lookup': {
        'from': 'customer',
        'localField': 'O_CUSTKEY',
        'foreignField': 'C_CUSTKEY',
        'as': 'customer'
    }
}

lookup_lineitem = {
    '$lookup': {
        'from': 'lineitem',
        'localField': 'O_ORDERKEY',
        'foreignField': 'L_ORDERKEY',
        'as': 'lineitem'
    }
}

mongo_pipe = [match_stage, lookup_customer, lookup_lineitem]

mongo_orders = mongo_db.orders.aggregate(mongo_pipe)
mongo_orders_list = list(mongo_orders)

# Process MongoDB data
orders_lineitem_df = pd.DataFrame([
    {
        'S_NATIONKEY': order['customer'][0]['C_NATIONKEY'],
        'revenue': sum(item['L_EXTENDEDPRICE'] * (1 - item['L_DISCOUNT']) for item in order['lineitem'])
    }
    for order in mongo_orders_list
    if 'customer' in order and order['customer']
])

revenue_df = orders_lineitem_df.groupby('S_NATIONKEY', as_index=False).sum()

# Merge MySQL and MongoDB data
result_df = pd.merge(mysql_df, revenue_df, left_on='S_NATIONKEY', right_on='S_NATIONKEY')

# Generate the final dataframe to export
final_df = result_df[['N_NAME', 'revenue']].sort_values(by='revenue', ascending=False)

# Write output to CSV
final_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close the connections
mysql_conn.close()
mongo_client.close()
