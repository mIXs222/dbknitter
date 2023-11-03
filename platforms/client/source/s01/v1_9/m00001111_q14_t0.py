import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
from pandas.io.sql import read_sql

# Function that executes the query and writes the result into a csv file
def execute_query():
    # Connection to MySQL
    engine = create_engine('mysql+mysqldb://root:my-secret-pw@mysql/tpch')

    # reading data from MySQL
    sql_query_part = "SELECT * FROM PART"
    df_part = read_sql(sql_query_part, con=engine)

    # Connection to MongoDB
    client = MongoClient('mongodb', 27017)
    db = client['tpch']

    # reading data from MongoDB
    cursor = db.lineitem.find()
    df_lineitem =  pd.DataFrame(list(cursor))

    # Merging two dataframes, i.e., joining lineitem and part
    merge_data = pd.merge(df_lineitem, df_part, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

    # filtering the rows based on shipdate
    before_date = pd.to_datetime('1995-09-01')
    after_date = pd.to_datetime('1995-10-01')
    date_filter = (merge_data['L_SHIPDATE'] >= before_date) & (merge_data['L_SHIPDATE'] < after_date)
    filtered_data = merge_data[date_filter]

    # Calculating the condition needed for SUM in the SQL query
    filtered_data['CONDITION'] = filtered_data.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']) if row['P_TYPE'].startswith('PROMO') else 0, axis=1)

    # Calculating the SUMs and generating the final output
    sum1 = filtered_data['CONDITION'].sum()
    sum2 = (filtered_data['L_EXTENDEDPRICE'] * (1 - filtered_data['L_DISCOUNT'])).sum()
    output = 100.00 * sum1 / sum2

    # Writing the output to a csv file
    pd.DataFrame([output]).to_csv('query_output.csv')

# Calling the function
execute_query()
