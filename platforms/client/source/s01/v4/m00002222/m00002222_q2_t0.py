import pymysql
import pandas as pd
import direct_redis

# Helper function to establish connection with MySQL
def mysql_connect():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch',
    )

# Helper function to fetch the data from Redis
def get_partsupp_data():
    dr = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    return pd.read_json(dr.get('partsupp'))

# Function to execute the overall logic
def execute_query():
    # Connect to the MySQL database
    mysql_conn = mysql_connect()
    with mysql_conn.cursor() as cursor:
        # Execute query for MySQL tables
        cursor.execute("""
        SELECT
            S_ACCTBAL,
            S_NAME,
            N_NAME,
            P_PARTKEY,
            P_MFGR,
            S_ADDRESS,
            S_PHONE,
            S_COMMENT
        FROM
            part,
            supplier,
            nation,
            region
        WHERE
            P_SIZE = 15
            AND P_TYPE LIKE '%BRASS'
            AND S_NATIONKEY = N_NATIONKEY
            AND N_REGIONKEY = R_REGIONKEY
            AND R_NAME = 'EUROPE'
        """)
        mysql_results = cursor.fetchall()
    # Convert MySQL data to DataFrame
    mysql_df = pd.DataFrame(list(mysql_results), columns=[
        "S_ACCTBAL", "S_NAME", "N_NAME", "P_PARTKEY", "P_MFGR", 
        "S_ADDRESS", "S_PHONE", "S_COMMENT"
    ])
    # Closing MySQL connection
    mysql_conn.close()

    # Fetch partsupp data from Redis
    partsupp_df = get_partsupp_data()

    # Merge the dataframes based on partkey and suppkey
    combined_df = pd.merge(
        mysql_df,
        partsupp_df,
        left_on=["P_PARTKEY", "S_SUPPKEY"],
        right_on=["PS_PARTKEY", "PS_SUPPKEY"]
    )
    # Filtering based on Europe region and minimum supply cost
    filtered_df = combined_df[combined_df['R_NAME'] == 'EUROPE']
    min_supply_cost = filtered_df['PS_SUPPLYCOST'].min()
    final_df = filtered_df[filtered_df['PS_SUPPLYCOST'] == min_supply_cost]

    # Sorting the dataframe
    final_df.sort_values(by=["S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY"], ascending=[False, True, True, True], inplace=True)

    # Writing the final dataframe to csv
    final_df.to_csv('query_output.csv', index=False)

# Main execution
if __name__ == '__main__':
    execute_query()
