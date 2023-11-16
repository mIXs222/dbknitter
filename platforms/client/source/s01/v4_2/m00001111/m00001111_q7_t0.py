import pymysql
import pymongo
import pandas as pd
from dateutil.parser import parse
from bson.code import Code

# Define the SQL and MongoDB connections
sql_conn = pymysql.connect(
  host='mysql',
  user='root',
  password='my-secret-pw',
  database='tpch'
)
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Execute SQL query and get data
sql_query = """
  SELECT
    N_NAME AS SUPP_NATION,
    S_SUPPKEY
  FROM
    supplier,
    nation
  WHERE S_NATIONKEY = N_NATIONKEY
    AND (N_NAME = 'JAPAN' OR N_NAME = 'INDIA')
"""
sql_df = pd.read_sql(sql_query, sql_conn)

# Define MongoDB map-reduce functions
map_func = Code("""
  function() {
    var fmtYear = function(d) { 
      return d.getFullYear(); 
    };
    var year = fmtYear(this.L_SHIPDATE);
    if (year >= '1995' && year <= '1996') {
      emit(
        {CUST_NATION: this.C_NATIONKEY, SUPPKEY: this.L_SUPPKEY}, 
        {VOLUME: this.L_EXTENDEDPRICE * (1 - this.L_DISCOUNT)}
      );
    }
  }
""")
reduce_func = Code("""
  function(key, values) {
    var total = 0;
    for (var i = 0; i < values.length; i++) {
      total += values[i].VOLUME;
    }
    return {VOLUME: total};
  }
""")

# Execute MongoDB map-reduce and get data
mongo_results = mongo_db.lineitem.map_reduce(map_func, reduce_func, "myresults")
mongo_df = pd.DataFrame(list(mongo_results.find()))

# Merge SQL and MongoDB data
df = pd.merge(sql_df, mongo_df, how='inner', left_on='S_SUPPKEY', right_on='_id.SUPPKEY')

# Group by SUPP_NATION, CUST_NATION and L_YEAR, then calculate sum of VOLUME as REVENUE
result_df = df.groupby(['SUPP_NATION', 'CUST_NATION', '_id.year'], as_index=False)['value.VOLUME'].sum()
result_df.rename(columns = {'_id.year':'L_YEAR', 'value.VOLUME':'REVENUE'}, inplace = True)

# Write result to CSV file
result_df.to_csv('query_output.csv', index=False)
