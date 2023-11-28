import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query execution
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT n.N_NAME, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
        FROM nation AS n
        JOIN supplier AS s ON s.S_NATIONKEY = n.N_NATIONKEY
        JOIN orders AS o ON o.O_ORDERDATE >= '1990-01-01' AND o.O_ORDERDATE <= '1994-12-31'
        JOIN lineitem AS l ON l.L_ORDERKEY = o.O_ORDERKEY AND l.L_SUPPKEY = s.S_SUPPKEY
        WHERE n.N_REGIONKEY = (
            SELECT r.R_REGIONKEY
            FROM region AS r 
            WHERE r.R_NAME = 'ASIA'
        )
        GROUP BY n.N_NAME
        ORDER BY revenue DESC;
    """)
    mysql_data = cursor.fetchall()
mysql_conn.close()

# Convert MySQL data to DataFrame
mysql_df = pd.DataFrame(mysql_data, columns=['N_NAME', 'revenue'])

# MongoDB connection and query execution
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client['tpch']
customer_col = mongodb_db['customer']
pipeline = [
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'C_CUSTKEY',
            'foreignField': 'O_CUSTKEY',
            'as': 'orders'
        }
    },
    {
        '$unwind': '$orders'
    },
    {
        '$match': {
            'orders.O_ORDERDATE': {
                '$gte': '1990-01-01',
                '$lte': '1994-12-31'
            }
        }
    },
    {
        '$lookup': {
            'from': 'nation',
            'localField': 'C_NATIONKEY',
            'foreignField': 'N_NATIONKEY',
            'as': 'nation'
        }
    },
    {
        '$unwind': '$nation'
    },
    {
        '$lookup': {
            'from': 'region',
            'localField': 'nation.N_REGIONKEY',
            'foreignField': 'R_REGIONKEY',
            'as': 'region'
        }
    },
    {
        '$match': {
            'region.R_NAME': 'ASIA'
        }
    },
    {
        '$project': {
            'nation.N_NAME': 1,
            'orders.O_ORDERKEY': 1
        }
    }
]
mongodb_data = list(customer_col.aggregate(pipeline))
mongodb_df = pd.DataFrame([{'N_NAME': item['nation'][0]['N_NAME'], 'O_ORDERKEY': item['orders']['O_ORDERKEY']} for item in mongodb_data])

redis_data = DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'lineitem' table from Redis and convert to DataFrame
redis_lineitem = redis_data.get('lineitem')
lineitem_df = pd.read_json(redis_lineitem)

# Calculate revenue for MongoDB and Redis DataFrames
combined_df = pd.merge(mongodb_df, lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
combined_df['revenue'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])
grouped_df = combined_df.groupby('N_NAME', as_index=False)['revenue'].sum().sort_values('revenue', ascending=False)

# Combine the results from MySQL and the combined MongoDB and Redis DataFrames
final_df = pd.concat([mysql_df, grouped_df]).groupby('N_NAME', as_index=False)['revenue'].sum().sort_values('revenue', ascending=False)

# Write the final DataFrame to CSV
final_df.to_csv('query_output.csv', index=False)
