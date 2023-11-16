import pymysql
import pandas as pd


def mysql_query(query, db_name, username, password, hostname):
    connection = pymysql.connect(
        host=hostname,
        user=username,
        password=password,
        db=db_name,
    )
    df = pd.read_sql_query(query, connection)
    connection.close()

    return df


db_config = {
    "db_name": "tpch",
    "username": "root",
    "password": "my-secret-pw",
    "hostname": "mysql",
}

tables = ["partsupp", "supplier", "nation"]
queries = [f"SELECT * FROM {table}" for table in tables]

dfs = [mysql_query(query, **db_config) for query in queries]
partsupp, supplier, nation = dfs

penultimate_df = pd.merge(partsupp, supplier, on='PS_SUPPKEY')
final_df = pd.merge(penultimate_df, nation, on='S_NATIONKEY')

filtered_df = final_df[final_df['N_NAME'] == 'GERMANY']

filtered_df['VALUE'] = filtered_df['PS_SUPPLYCOST'] * filtered_df['PS_AVAILQTY']

grouped_df = filtered_df.groupby('PS_PARTKEY')['VALUE'].sum().reset_index()

grouped_df = grouped_df[grouped_df['VALUE'] > grouped_df['VALUE'].sum() * 0.0001000000]

sorted_df = grouped_df.sort_values('VALUE', ascending=False)

sorted_df.to_csv('query_output.csv', index=False)
