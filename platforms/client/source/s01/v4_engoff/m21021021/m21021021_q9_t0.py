# Python code to execute the query
import pandas as pd
import pymysql
from pymongo import MongoClient
import direct_redis


# Function to connect to MySQL and execute the query
def get_mysql_data(specified_dim):
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch',
                                 cursorclass=pymysql.cursors.Cursor)
    try:
        with connection.cursor() as cursor:
            query = """
            SELECT
                n.N_NAME AS nation,
                YEAR(o.O_ORDERDATE) AS o_year,
                SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) AS profit
            FROM
                part p
                JOIN lineitem l ON p.P_PARTKEY = l.L_PARTKEY
                JOIN partsupp ps ON l.L_PARTKEY = ps.PS_PARTKEY AND l.L_SUPPKEY = ps.PS_SUPPKEY
                JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
                JOIN supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
                JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
            WHERE
                p.P_NAME LIKE %s
            GROUP BY
                nation,
                o_year
            ORDER BY
                nation ASC,
                o_year DESC
            """
            cursor.execute(query, ('%' + specified_dim + '%',))
            data = cursor.fetchall()
    finally:
        connection.close()
    return pd.DataFrame(data, columns=['nation', 'o_year', 'profit'])


# Function to connect to MongoDB and execute the query
def get_mongodb_data(specified_dim):
    client = MongoClient('mongodb', 27017)
    db = client['tpch']
    partsupp = pd.DataFrame(list(db.partsupp.find()))
    lineitem = pd.DataFrame(list(db.lineitem.find()))
    # Perform the query equivalent in pandas
    part_df = lineitem[lineitem['L_COMMENT'].str.contains(specified_dim)]
    merge_df = part_df.merge(partsupp, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
    merge_df['profit'] = (merge_df['L_EXTENDEDPRICE'] * (1 - merge_df['L_DISCOUNT'])) - (merge_df['PS_SUPPLYCOST'] * merge_df['L_QUANTITY'])
    profit_by_year_and_nation = merge_df.groupby(['N_NAME', 'O_YEAR'])['profit'].sum().reset_index()
    profit_by_year_and_nation = profit_by_year_and_nation.sort_values(['N_NAME', 'O_YEAR'], ascending=[True, False])
    return profit_by_year_and_nation


# Function to connect to Redis and read the data from it
def get_redis_data():
    redis_client = direct_redis.DirectRedis(port=6379, host='redis')
    df_nation = pd.read_msgpack(redis_client.get('nation'))
    df_supplier = pd.read_msgpack(redis_client.get('supplier'))
    df_orders = pd.read_msgpack(redis_client.get('orders'))
    return df_nation, df_supplier, df_orders


# Main function to combine data from different sources and write to a CSV file
def main():
    specified_dim = "specific_dim"  # Replace with the actual dimension specified in the query
    mysql_data = get_mysql_data(specified_dim)
    mongodb_data = get_mongodb_data(specified_dim)
    
    # Get the Redis data, assuming they are used in the MongoDB part of the query
    df_nation, df_supplier, df_orders = get_redis_data()
    
    # Merge the two sources of data based on the common schema provided in the query
    final_data = mysql_data.merge(mongodb_data, on=['nation', 'o_year'], how='outer')
    
    # Aggregating profit from both sources and sorting as required
    final_data['total_profit'] = final_data['profit_x'].fillna(0) + final_data['profit_y'].fillna(0)
    final_data.sort_values(['nation', 'o_year'], ascending=[True, False], inplace=True)
    
    # Write the final result to a CSV file
    final_data.to_csv('query_output.csv', index=False)


if __name__ == '__main__':
    main()
