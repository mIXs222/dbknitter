import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query execution
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
with mysql_conn.cursor() as my_cursor:
    my_cursor.execute("""
        SELECT s.S_NATIONKEY, n.N_NAME, o.O_ORDERDATE, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT))
        FROM supplier AS s
        JOIN nation AS n ON s.S_NATIONKEY = n.N_NATIONKEY
        JOIN orders AS o ON o.O_ORDERKEY = l.L_ORDERKEY
        WHERE n.N_NAME IN ('INDIA', 'JAPAN') AND 
              YEAR(o.O_ORDERDATE) IN (1995, 1996) AND 
              o.O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
        GROUP BY s.S_NATIONKEY, n.N_NAME, YEAR(o.O_ORDERDATE)
    """)
    mysql_data = pd.DataFrame(my_cursor.fetchall())
mysql_conn.close()

# Adjust columns based on fetched data
mysql_data.columns = ['S_NATIONKEY', 'N_NAME', 'O_ORDERDATE', 'REVENUE']
mysql_data['YEAR'] = pd.DatetimeIndex(mysql_data['O_ORDERDATE']).year

# MongoDB connection and query execution
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_data = pd.DataFrame(list(mongo_db.customer.find({'C_NATIONKEY': {"$in": [1, 3]}})))

mongo_client.close()

# Redis connection and data fetching
redis_conn = DirectRedis(host='redis', port=6379, db=0)
lineitem_data = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Combine the tables
combined_data = (
    mysql_data.merge(lineitem_data, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(mongo_data, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
)

# Generating the desired output dataframe with proper filters and group by clauses
output_data = combined_data.loc[
    ((combined_data.N_NAME == 'INDIA') & (combined_data.C_NATIONKEY == 3)) |
    ((combined_data.N_NAME == 'JAPAN') & (combined_data.C_NATIONKEY == 1))
].groupby(['S_NATIONKEY', 'C_NATIONKEY', 'YEAR']).agg({'REVENUE': 'sum'}).reset_index()

output_data.sort_values(['S_NATIONKEY', 'C_NATIONKEY', 'YEAR'], ascending=[True, True, True], inplace=True)

# Saving output to CSV
output_data.to_csv('query_output.csv', index=False)
