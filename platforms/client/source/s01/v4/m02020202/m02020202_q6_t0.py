# query.py
import pandas as pd
from direct_redis import DirectRedis

def execute_query():
    # Connection information for Redis
    redis_host = 'redis'
    redis_port = 6379
    redis_db = 0

    # Establish the connection to the Redis database
    redis_conn = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

    # Get the 'lineitem' DataFrame from Redis
    lineitem = pd.read_msgpack(redis_conn.get('lineitem'))

    # Filtering the lineitem DataFrame according to the SQL WHERE clause
    filtered_lineitem = lineitem[
        (lineitem['L_SHIPDATE'] >= pd.Timestamp('1994-01-01')) &
        (lineitem['L_SHIPDATE'] < pd.Timestamp('1995-01-01')) &
        (lineitem['L_DISCOUNT'] >= (.06 - 0.01)) &
        (lineitem['L_DISCOUNT'] <= (.06 + 0.01)) &
        (lineitem['L_QUANTITY'] < 24)
    ]

    # Calculating the REVENUE as per the SQL SELECT clause
    result = pd.DataFrame({
        'REVENUE': [(filtered_lineitem['L_EXTENDEDPRICE'] * filtered_lineitem['L_DISCOUNT']).sum()]
    })

    # Writing the results to query_output.csv
    result.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    execute_query()
