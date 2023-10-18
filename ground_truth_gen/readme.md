This is to generate tpch_sqlite ground truth for all queries

1. to create the sqlite database, define all the schema and import all the data, run:
   ```
   make data_dir=../data
   ```
   where data_dir parameter specifies the data dirctory
2. to generate the result of all the tpc-h queries, run:
   ```
   ./execute_queries.sh
   ```
3. to generate the result of a single query (e.g. q15), run:
   ```
   ./execute_single_query.sh tpch-queries/q15.sql
   ```
