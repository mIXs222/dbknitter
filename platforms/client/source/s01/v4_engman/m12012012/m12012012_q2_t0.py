import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis


def connect_mysql():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        database='tpch'
    )


def query_mysql():
    connection = connect_mysql()
    query = """
        SELECT P_PARTKEY, P_MFGR
        FROM part
        WHERE P_TYPE = 'BRASS' AND P_SIZE = 15
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['P_PARTKEY', 'P_MFGR'])
            return df
    finally:
        connection.close()


def connect_mongodb():
    client = pymongo.MongoClient("mongodb", 27017)
    return client['tpch']


def query_mongodb():
    db = connect_mongodb()
    nation_data = pd.DataFrame(list(db['nation'].find({'N_NAME': 'EUROPE'})))
    supplier_data = pd.DataFrame(list(db['supplier'].find()))
    return nation_data, supplier_data


def connect_redis():
    return DirectRedis(host='redis', port=6379, db=0)


def query_redis():
    redis_client = connect_redis()
    region_data = pd.read_json(redis_client.get('region'))
    partsupp_data = pd.read_json(redis_client.get('partsupp'))
    return region_data, partsupp_data


def main():
    part_data = query_mysql()
    nation_data, supplier_data = query_mongodb()
    region_data, partsupp_data = query_redis()

    europe_nations = nation_data[nation_data['N_NAME'] == 'EUROPE']

    result = (
        partsupp_data[partsupp_data['PS_PARTKEY'].isin(part_data['P_PARTKEY'])]
        .merge(supplier_data, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
        .merge(europe_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
        .merge(part_data, left_on='PS_PARTKEY', right_on='P_PARTKEY')
        .sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])
    )

    # Select only the first row for each part as it has the minimum cost after sorting
    result = result.groupby('P_PARTKEY').first().reset_index()

    # Choose columns in the specific order requested and write to CSV
    result = result[['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]
    result.to_csv('query_output.csv', index=False)


if __name__ == "__main__":
    main()
