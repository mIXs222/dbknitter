import pandas as pd
import pymysql.cursors
import csv

def connect_mysql():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

def get_data_from_mysql():

    try:
        conn = connect_mysql()
        
        with conn.cursor() as cursor:
            
            sql = """
            SELECT
                C_CUSTKEY,
                COUNT(O_ORDERKEY) AS C_COUNT
            FROM
                customer LEFT OUTER JOIN orders ON
                C_CUSTKEY = O_CUSTKEY
                AND O_COMMENT NOT LIKE '%pendings%deposits%'
            GROUP BY
                C_CUSTKEY"""
            
            cursor.execute(sql)
            
            result = cursor.fetchall()
            df = pd.DataFrame(result)
            
            return df

    finally:
        conn.close()

def process_data(df):
    df_output = df.groupby('C_COUNT').size().reset_index(name='CUSTDIST')
    df_output = df_output.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

    return df_output

def main():
    
    mysql_data = get_data_from_mysql()
    output_data = process_data(mysql_data)

    output_data.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
    
if __name__ == "__main__":
    main()
