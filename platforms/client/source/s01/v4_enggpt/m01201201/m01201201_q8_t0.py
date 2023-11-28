import pandas as pd
import pymysql
from pymongo import MongoClient
from direct_redis import DirectRedis


# Connecting to MySQL
def connect_mysql():
    return pymysql.connect(host='mysql',
                           user='root',
                           password='my-secret-pw',
                           db='tpch',
                           cursorclass=pymysql.cursors.Cursor)


def query_mysql():
    mysql_conn = connect_mysql()
    try:
        with mysql_conn.cursor() as cursor:
            query = """
                SELECT o.O_ORDERDATE, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as volume
                FROM nation n
                JOIN supplier s ON n.N_NATIONKEY = s.S_NATIONKEY
                JOIN orders o ON s.S_SUPPKEY = o.O_CUSTKEY
                JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
                WHERE r.R_NAME = 'ASIA' AND n.N_NAME = 'INDIA' AND o.O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
                GROUP BY o.O_ORDERDATE
            """
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        mysql_conn.close()
    return result


# Connecting to MongoDB
def connect_mongodb():
    return MongoClient(host='mongodb', port=27017)


def query_mongodb(part_type):
    mongo_client = connect_mongodb()
    db = mongo_client['tpch']
    lineitem_df = pd.DataFrame(list(db.lineitem.find(
        {"L_SHIPDATE": {"$gte": "1995-01-01", "$lte": "1996-12-31"}}
    )))
    part_df = pd.DataFrame(list(db.part.find(
        {"P_TYPE": part_type}
    )))
    mongo_client.close()
    return lineitem_df, part_df


# Connecting to Redis
def connect_redis():
    return DirectRedis(host='redis', port=6379, db=0)


def query_redis():
    redis_conn = connect_redis()
    part_df = pd.DataFrame([eval(redis_conn.get('part'))])
    customer_df = pd.DataFrame([eval(redis_conn.get('customer'))])
    return part_df, customer_df


def main():
    # Query mysql for orders and volumes
    mysql_data = query_mysql()

    # Query mongodb for lineitem
    part_type = "SMALL PLATED COPPER"
    lineitem_df, part_df = query_mongodb(part_type)

    # Query redis for part and customer
    part_redis_df, customer_redis_df = query_redis()

    # Assuming that the part and customer dataframes are in the same format as those in mongodb,
    # otherwise additional processing would be required.
    part_df = pd.concat([part_df, part_redis_df], ignore_index=True)
    customer_df = pd.concat([customer_redis_df], ignore_index=True)

    # Merge all dataframes to calculate the market share
    # NOTE: This involves assuming a consistent schema and joining logic between the databases.
    # In a real-world scenario, we'd need to handle inconsistencies and possibly normalize data.
    merged_df = pd.merge(lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
    merged_df = pd.merge(merged_df, customer_df, left_on='L_ORDERKEY', right_on='C_CUSTKEY')
    merged_df = pd.DataFrame(mysql_data, columns=['O_ORDERDATE', 'volume'])

    # Compute market share
    merged_df['year'] = pd.to_datetime(merged_df['O_ORDERDATE']).dt.year
    india_volume = merged_df.groupby('year')['volume'].sum()
    total_volume = merged_df['volume'].sum()
    market_share = india_volume / total_volume

    # Write to file
    market_share_df = market_share.reset_index()
    market_share_df.columns = ['year', 'market_share']
    market_share_df.sort_values('year', ascending=True).to_csv('query_output.csv', index=False)


if __name__ == "__main__":
    main()
