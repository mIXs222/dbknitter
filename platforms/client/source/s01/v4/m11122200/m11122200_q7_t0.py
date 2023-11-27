# Import required libraries
import pandas as pd
import pymysql
import pymongo
import direct_redis

# Function to establish a connection to the MySQL database and execute the query partially
def mysql_query():
    HOST = "mysql"
    USER = "root"
    PASSWORD = "my-secret-pw"
    DATABASE = "tpch"

    # Connect to MySQL database
    conn = pymysql.connect(host=HOST, user=USER, password=PASSWORD, database=DATABASE)
    
    # MySQL query
    mysql_query = """
        SELECT 
            O_ORDERKEY, 
            L_ORDERKEY, 
            L_EXTENDEDPRICE, 
            L_DISCOUNT, 
            L_SHIPDATE 
        FROM 
            orders, 
            lineitem 
        WHERE 
            O_ORDERKEY=L_ORDERKEY AND 
            L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
    """
    
    # Execute the query and fetch all the results
    with conn.cursor() as cursor:
        cursor.execute(mysql_query)
        result = cursor.fetchall()

    # Create a DataFrame from the result
    columns = ['O_ORDERKEY', 'L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SHIPDATE']
    lineitem_orders_df = pd.DataFrame(result, columns=columns)

    conn.close()

    return lineitem_orders_df

# Function to fetch data from MongoDB
def mongodb_query():
    HOST = "mongodb"
    PORT = 27017
    DATABASE = "tpch"

    # Connect to MongoDB database
    client = pymongo.MongoClient(host=HOST, port=PORT)
    db = client[DATABASE]

    # MongoDB query
    nation_docs = db.nation.find({})

    # Create a DataFrame from the query results
    nation_df = pd.DataFrame(list(nation_docs))

    client.close()

    return nation_df

# Function to fetch data from Redis and convert it to Pandas DataFrame
def redis_query(tablename):
    HOST = "redis"
    PORT = 6379
    DATABASE = 0

    # Connect to Redis
    redis_conn = direct_redis.DirectRedis(host=HOST, port=PORT, db=DATABASE)
    
    # Fetch the data from Redis as a string and convert it to a DataFrame
    data = redis_conn.get(tablename)
    df = pd.read_json(data)

    return df

def main():
    # Fetch data from different databases
    lineitem_orders_df = mysql_query()
    nation_df = mongodb_query()
    supplier_df = redis_query('supplier')
    customer_df = redis_query('customer')

    # Calculate the derived columns with correct type conversion for dates
    lineitem_orders_df['L_YEAR'] = pd.to_datetime(lineitem_orders_df['L_SHIPDATE']).dt.year
    lineitem_orders_df['VOLUME'] = lineitem_orders_df['L_EXTENDEDPRICE'] * (1 - lineitem_orders_df['L_DISCOUNT'])
    
    # Merge all dataframes
    merged_df = (
        lineitem_orders_df
        .merge(supplier_df, left_on='L_ORDERKEY', right_on='S_SUPPKEY', how='inner')
        .merge(customer_df, left_on='O_ORDERKEY', right_on='C_CUSTKEY', how='inner')
        .merge(nation_df.rename(columns={'N_NAME': 'SUPP_NATION', 'N_NATIONKEY': 'S_NATIONKEY'}), on='S_NATIONKEY', how='inner')
        .merge(nation_df.rename(columns={'N_NAME': 'CUST_NATION', 'N_NATIONKEY': 'C_NATIONKEY'}), on='C_NATIONKEY', how='inner')
    )

    # Filter the final DataFrame for the required conditions
    filtered_df = merged_df[
        ((merged_df['SUPP_NATION'] == 'JAPAN') & (merged_df['CUST_NATION'] == 'INDIA')) |
        ((merged_df['SUPP_NATION'] == 'INDIA') & (merged_df['CUST_NATION'] == 'JAPAN'))
    ]

    # Group by nation, customer nation, and year
    final_df = (
        filtered_df.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])
        .agg({'VOLUME': 'sum'})
        .reset_index()
        .sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])
    )
    
    # Write the results to a CSV file
    final_df.to_csv('query_output.csv', index=False)


if __name__ == "__main__":
    main()
