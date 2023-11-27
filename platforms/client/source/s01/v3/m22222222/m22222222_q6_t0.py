import csv
import direct_redis
import pandas as pd 

def get_redis_data():
    # establishing connection
    r = direct_redis.Direct_Redis(host='redis', port=6379, db=0)

    # reading the table from redis database
    data = r.get_pandas_dataframe_by_key('lineitem')

    # filtering the data
    data['L_SHIPDATE'] = pd.to_datetime(data['L_SHIPDATE'])
    data = data[ (data.L_SHIPDATE >= '1994-01-01') & (data.L_SHIPDATE < '1995-01-01') & 
                 (data.L_DISCOUNT.between(.06 - 0.01, .06 + 0.01)) & (data.L_QUANTITY < 24) ]

    # calculating the revenue
    data['REVENUE'] = data['L_EXTENDEDPRICE']* data['L_DISCOUNT']
    total_revenue = sum(data['REVENUE'])

    return total_revenue

# calling the redis function
result = get_redis_data()

# writing to the file 
with open("query_output.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["REVENUE"])
    writer.writerow([result])
