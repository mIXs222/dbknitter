import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to get data from MySQL
def get_mysql_data(part_name_dim):
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    try:
        with connection.cursor() as cursor:
            query = f"""
                SELECT
                    n.N_NAME AS nation,
                    YEAR(l.L_SHIPDATE) AS year,
                    SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) AS profit
                FROM
                    partsupp ps
                JOIN
                    lineitem l ON l.L_PARTKEY = ps.PS_PARTKEY AND l.L_SUPPKEY = ps.PS_SUPPKEY
                JOIN
                    supplier s ON s.S_SUPPKEY = ps.PS_SUPPKEY
                JOIN
                    nation n ON s.S_NATIONKEY = n.N_NATIONKEY
                JOIN
                    part p ON p.P_PARTKEY = l.L_PARTKEY
                WHERE
                    p.P_NAME LIKE %s
                GROUP BY
                    nation, year
                ORDER BY
                    nation ASC, year DESC;
            """
            cursor.execute(query, ("%" + part_name_dim + "%",))
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['nation', 'year', 'profit'])
    finally:
        connection.close()
    return df

# Function to get MongoDB data
def get_mongodb_data(part_name_dim):
    client = pymongo.MongoClient("mongodb", 27017)
    db = client.tpch
    pipeline = [
        {"$match": {"P_NAME": {"$regex": part_name_dim}}},
        {"$project": {"P_PARTKEY": 1}}
    ]
    part_keys = list(db.part.aggregate(pipeline))
    part_keys = [doc['P_PARTKEY'] for doc in part_keys]
    return part_keys

# Function to get Redis data
def get_redis_df(key):
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    data = redis_client.get(key)
    if data:
        df = pd.read_json(data, orient='records')
        return df
    else:
        return pd.DataFrame()

# Main function
def main():
    # Define the part name dimension
    part_name_dim = 'specified dim'

    # Get data from MongoDB to filter parts that contain the specified dim
    part_keys = get_mongodb_data(part_name_dim)

    # Continue with MySQL part if we got part keys (assuming that the part keys found in mongo are used to filter MySQL data)
    if part_keys:
        # Get MySQL data
        df_mysql = get_mysql_data(part_name_dim)

        # Filter the MySQL data
        df_mysql_filtered = df_mysql[df_mysql['l_partkey'].isin(part_keys)]
        
        # Get Redis data for nation
        df_nation = get_redis_df('nation')
        df_supplier = get_redis_df('supplier')
        df_orders = get_redis_df('orders')
        
        # Merge Redis dataframes
        df_redis_nation_supplier = pd.merge(df_supplier, df_nation, left_on='s_nationkey', right_on='n_nationkey', how='inner')
        
        # Merge the MySQL and Redis dataframes
        df_result = pd.merge(df_mysql_filtered, df_redis_nation_supplier, left_on='ps_suppkey', right_on='s_suppkey', how='inner')

        # Calculate profit
        df_result['profit'] = (df_result['l_extendedprice'] * (1 - df_result['l_discount'])) - (df_result['ps_supplycost'] * df_result['l_quantity'])
        df_profit_nation_year = df_result.groupby(['nation', 'year']).agg({'profit': 'sum'}).reset_index()

        # Sort the final output
        df_profit_nation_year_sorted = df_profit_nation_year.sort_values(by=['nation', 'year'], ascending=[True, False])

        # Write to CSV
        df_profit_nation_year_sorted.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
