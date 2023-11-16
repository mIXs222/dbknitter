#!/bin/bash
int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 2 ]
then
  echo "Require 2 argument (DATA_ROOT, TABLES), $# provided"
  echo "Example: tpch_init.sh /path/to/data nation,region,part"
  echo "Example: tpch_init.sh /path/to/data nation,region,part,supplier,partsupp,customer,orders,lineitem"
  exit 1
fi

DATA_ROOT=$1
TABLES_STR=$2
TABLES=$(echo $TABLES_STR | tr "," "\n")
echo "Using DATA_ROOT=${DATA_ROOT}, TABLES=${TABLES_STR}"

read -r -d '' HEADER <<- EOM
import direct_redis
import pandas as pd

redis_client = direct_redis.DirectRedis(host='localhost', port=6379, db=0, ssl=False)
EOM

read -r -d '' TAIL <<- EOM
redis_client.set(f"{table_name}", df)
print(f"Inserted {df.shape} dataframe")
EOM

read -r -d '' NATION_PY <<- EOM
${HEADER}
table_name = "nation"
df = pd.read_csv('${DATA_ROOT}/nation.tbl', sep='|', header=None)
df = df.drop(len(df.columns) - 1, axis=1)
df.columns = [
    'N_NATIONKEY',
    'N_NAME',
    'N_REGIONKEY',
    'N_COMMENT',
]
${TAIL}
EOM

read -r -d '' REGION_PY <<- EOM
${HEADER}
table_name = "region"
df = pd.read_csv('${DATA_ROOT}/region.tbl', sep='|', header=None)
df = df.drop(len(df.columns) - 1, axis=1)
df.columns = [
    'R_REGIONKEY',
    'R_NAME',
    'R_COMMENT',
]
${TAIL}
EOM

read -r -d '' PART_PY <<- EOM
${HEADER}
table_name = "part"
df = pd.read_csv('${DATA_ROOT}/part.tbl', sep='|', header=None)
df = df.drop(len(df.columns) - 1, axis=1)
df.columns = [
    'P_PARTKEY',
    'P_NAME',
    'P_MFGR',
    'P_BRAND',
    'P_TYPE',
    'P_SIZE',
    'P_CONTAINER',
    'P_RETAILPRICE',
    'P_COMMENT',
]
${TAIL}
EOM

read -r -d '' SUPPLIER_PY <<- EOM
${HEADER}
table_name = "supplier"
df = pd.read_csv('${DATA_ROOT}/supplier.tbl', sep='|', header=None)
df = df.drop(len(df.columns) - 1, axis=1)
df.columns = [
    'S_SUPPKEY',
    'S_NAME',
    'S_ADDRESS',
    'S_NATIONKEY',
    'S_PHONE',
    'S_ACCTBAL',
    'S_COMMENT',
]
${TAIL}
EOM

read -r -d '' PARTSUPP_PY <<- EOM
${HEADER}
table_name = "partsupp"
df = pd.read_csv('${DATA_ROOT}/partsupp.tbl', sep='|', header=None)
df = df.drop(len(df.columns) - 1, axis=1)
df.columns = [
    'PS_PARTKEY',
    'PS_SUPPKEY',
    'PS_AVAILQTY',
    'PS_SUPPLYCOST',
    'PS_COMMENT',
]
${TAIL}
EOM

read -r -d '' CUSTOMER_PY <<- EOM
${HEADER}
table_name = "customer"
df = pd.read_csv('${DATA_ROOT}/customer.tbl', sep='|', header=None)
df = df.drop(len(df.columns) - 1, axis=1)
df.columns = [
    'C_CUSTKEY',
    'C_NAME',
    'C_ADDRESS',
    'C_NATIONKEY',
    'C_PHONE',
    'C_ACCTBAL',
    'C_MKTSEGMENT',
    'C_COMMENT',
]
${TAIL}
EOM

read -r -d '' ORDERS_PY <<- EOM
${HEADER}
table_name = "orders"
df = pd.read_csv('${DATA_ROOT}/orders.tbl', sep='|', header=None)
df = df.drop(len(df.columns) - 1, axis=1)
df.columns = [
    'O_ORDERKEY',
    'O_CUSTKEY',
    'O_ORDERSTATUS',
    'O_TOTALPRICE',
    'O_ORDERDATE',
    'O_ORDERPRIORITY',
    'O_CLERK',
    'O_SHIPPRIORITY',
    'O_COMMENT',
]
${TAIL}
EOM

read -r -d '' LINEITEM_PY <<- EOM
${HEADER}
table_name = "lineitem"
df = pd.read_csv('${DATA_ROOT}/lineitem.tbl', sep='|', header=None)
df = df.drop(len(df.columns) - 1, axis=1)
df.columns = [
    'L_ORDERKEY',
    'L_PARTKEY',
    'L_SUPPKEY',
    'L_LINENUMBER',
    'L_QUANTITY',
    'L_EXTENDEDPRICE',
    'L_DISCOUNT',
    'L_TAX',
    'L_RETURNFLAG',
    'L_LINESTATUS',
    'L_SHIPDATE',
    'L_COMMITDATE',
    'L_RECEIPTDATE',
    'L_SHIPINSTRUCT',
    'L_SHIPMODE',
    'L_COMMENT',
]
${TAIL}
EOM

echo "Initializing MongoDB"

. /venv/bin/activate
for table in ${TABLES}; do
    echo "============================="
    echo "Loading ${table}..."

    if [[ $table == "nation" ]]
    then
        python3 -c "${NATION_PY}"
    elif [[ $table == "region" ]]
    then
        python3 -c "${REGION_PY}"
    elif [[ $table == "part" ]]
    then
        python3 -c "${PART_PY}"
    elif [[ $table == "supplier" ]]
    then
        python3 -c "${SUPPLIER_PY}"
    elif [[ $table == "partsupp" ]]
    then
        python3 -c "${PARTSUPP_PY}"
    elif [[ $table == "customer" ]]
    then
        python3 -c "${CUSTOMER_PY}"
    elif [[ $table == "orders" ]]
    then
        python3 -c "${ORDERS_PY}"
    elif [[ $table == "lineitem" ]]
    then
        python3 -c "${LINEITEM_PY}"
    else
        echo "ERROR: Invalid table name ${table}"
    fi
done
