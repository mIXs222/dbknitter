import pandas as pd
import redis
from direct_redis import DirectRedis


def read_data_from_redis(db_number: int, host: str, port: int, table_name: str) -> pd.DataFrame:
    # Initiate connection to redis
    r = redis.Redis(db=db_number, host=host, port=port, decode_responses=True)
    direct_redis = DirectRedis(connection=r)
    data = direct_redis.get(table_name)
    return pd.DataFrame.from_records(data)


def main():
    # Read data from redis
    customer_df = read_data_from_redis(0, "redis", 6379, "customer")
    orders_df = read_data_from_redis(0, "redis", 6379, "orders")
    lineitem_df = read_data_from_redis(0, "redis", 6379, "lineitem")

    # Merge data frames
    data = pd.merge(customer_df, orders_df, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
    data = pd.merge(data, lineitem_df, left_on="O_ORDERKEY", right_on="L_ORDERKEY")

    # Filter results
    data = data[
        (data['C_MKTSEGMENT'] == 'BUILDING')
        & (data['O_ORDERDATE'] < '1995-03-15')
        & (data['L_SHIPDATE'] > '1995-03-15')
    ]

    # Generate results and sort
    data['REVENUE'] = data['L_EXTENDEDPRICE'] * (1 - data['L_DISCOUNT'])
    result = data.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).sum()
    result = result.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

    # Save results
    result.to_csv('query_output.csv')


if __name__ == "__main__":
    main()
