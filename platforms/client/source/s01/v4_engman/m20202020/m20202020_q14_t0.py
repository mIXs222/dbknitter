# promotion_effect_query.py
import pymysql
import pandas as pd
import direct_redis

# Function to get the lineitem data from MySQL database
def get_mysql_data(connection_info):
    conn = pymysql.connect(host=connection_info['hostname'],
                           user=connection_info['username'],
                           password=connection_info['password'],
                           db=connection_info['database'])
    query = """
    SELECT L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT
    FROM lineitem
    WHERE L_SHIPDATE >= '1995-09-01' AND L_SHIPDATE < '1995-10-01'
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Function to get the part data from Redis
def get_redis_data(connection_info):
    client = direct_redis.DirectRedis(host=connection_info['hostname'], port=connection_info['port'], db=connection_info['database'])
    part_df = pd.read_json(client.get('part'), orient='index')
    return part_df

# Combining data from different sources
def combine_data(lineitem_df, part_df):
    # Compute revenue for each line item
    lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
    # Join both tables on partkey
    merged_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
    
    # Compute the total revenue
    total_revenue = merged_df['REVENUE'].sum()
    # Compute promotion revenue
    promo_revenue = merged_df[merged_df['P_RETAILPRICE'] > 0]['REVENUE'].sum()
    
    # Compute promotion effect percentage
    promotion_effect = (promo_revenue / total_revenue) * 100 if total_revenue else 0
    return promotion_effect

def main():
    mysql_info = {
        "database": "tpch",
        "username": "root",
        "password": "my-secret-pw",
        "hostname": "mysql"
    }

    redis_info = {
        "database": 0,
        "port": 6379,
        "hostname": "redis"
    }

    # Get data from both databases
    lineitem_data = get_mysql_data(mysql_info)
    part_data = get_redis_data(redis_info)

    # Combine data and calculate promotion effect
    promotion_effect_percentage = combine_data(lineitem_data, part_data)

    # Save the result to a CSV file
    pd.DataFrame({'Promotion Effect (%)': [promotion_effect_percentage]}).to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
