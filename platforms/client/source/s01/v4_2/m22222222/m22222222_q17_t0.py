import redis
import pandas as pd
import os
from pandasql import sqldf

#Function to run sql on pandas dataframe
pysqldf = lambda q: sqldf(q, globals())

redis_host = "redis"
redis_port = 6379
redis_password = ""
try:
    r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)

    # Read data from redis
    part = r.get('part')
    lineitem = r.get('lineitem')

    part_df = pd.read_json(part)
    lineitem_df = pd.read_json(lineitem)
    
    # Register these tables in the local scope
    locals().update({'part': part_df, 'lineitem': lineitem_df})
    
    # SQL Query
    sql="""
    SELECT
         SUM(L_EXTENDEDPRICE)/7.0 as AVG_YEARLY
    FROM
         lineitem
    JOIN 
         part
    ON    
        P_PARTKEY = L_PARTKEY
    WHERE
        P_BRAND = 'Brand#23'
        AND P_CONTAINER = 'MED BAG'
        AND L_QUANTITY < (
             SELECT
                 0.2*AVG(L_QUANTITY)
             FROM 
                 lineitem
             WHERE 
                 L_PARTKEY = P_PARTKEY
                  )
    """
  
    # Execute SQL on Pandas DataFrame
    result = pysqldf(sql)
    result.to_csv("query_output.csv", index=False)
  
except Exception as e:
    print(e)
