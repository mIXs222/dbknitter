import pymysql.cursors
import pandas as pd

def connect_to_mysql(db_name: str, host: str, user: str, password: str):
    connection = pymysql.connect(host=host,
                                 user=user,
                                 password=password,
                                 db=db_name,
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection
   
def execute_query(connection, query: str):
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    return result

mysql_conn = connect_to_mysql('tpch', 'mysql', 'root', 'my-secret-pw')

# Load both tables
lineitem_query = "SELECT * FROM lineitem"
part_query = "SELECT * FROM part"

lineitem_data = pd.DataFrame(execute_query(mysql_conn, lineitem_query))
part_data = pd.DataFrame(execute_query(mysql_conn, part_query))

# Merge dataframes based on L_PARTKEY and P_PARTKEY
 merged_data = pd.merge(lineitem_data, part_data, how='inner', left_on="L_PARTKEY", right_on="P_PARTKEY")
  
# Filter based on given criteria
filtered_data = merged_data[(merged_data.L_SHIPDATE >= '1995-09-01') & (merged_data.L_SHIPDATE < '1995-10-01')]

# Calculated SUM as per the given criteria
promo_sum = filtered_data.loc[filtered_data.P_TYPE.str.contains('PROMO'), 'L_EXTENDEDPRICE'].sum() * (1 - filtered_data.loc[filtered_data.P_TYPE.str.contains('PROMO'), 'L_DISCOUNT'].sum())
total_sum = filtered_data['L_EXTENDEDPRICE'].sum() * (1 - filtered_data['L_DISCOUNT'].sum())

final_result = 100.00 * (promo_sum / total_sum)

# Write results to csv file
final_result.to_csv('query_output.csv')
