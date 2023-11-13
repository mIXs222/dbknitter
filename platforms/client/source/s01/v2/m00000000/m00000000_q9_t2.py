from sqlalchemy import create_engine
import pandas as pd

def execute_query():
    db_config = {
        'username': 'root',
        'password': 'my-secret-pw',
        'hostname': 'mysql',
        'database': 'tpch',
    }

    # MySQL
    query = '''
            Your SQL query here
            '''
    mysql_conn_str = f'mysql+pymysql://{db_config["username"]}:{db_config["password"]}@{db_config["hostname"]}/{db_config["database"]}'
    engine = create_engine(mysql_conn_str)
    with engine.connect() as connection:
        result = connection.execute(query)
        df = pd.DataFrame(result.fetchall())
        df.columns = result.keys()

    # Write output to .csv file
    df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    execute_query()
