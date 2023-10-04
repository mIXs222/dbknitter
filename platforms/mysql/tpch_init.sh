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
  echo "Example: tpch_init.sh /path/to/data nation,lineitem,part"
  exit 1
fi

PASSWORD=password  # TODO: change this

DATA_ROOT=$1
TABLES_STR=$2
TABLES=$(echo $TABLES_STR | tr "," "\n")
echo "Using DATA_ROOT=${DATA_ROOT}, TABLES=${TABLES_STR}"

echo "Initializing MySQL"
mysql -p${PASSWORD} -e "CREATE DATABASE tpch; USE tpch;"

for table in ${TABLES}; do
    echo "============================="
    echo "Loading ${table}..."

    if [[ $table == "nation" ]]
    then
        mysql -p${PASSWORD} --local-infile tpch -e "
            SET GLOBAL local_infile=1;
            CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                                N_NAME       CHAR(25) NOT NULL,
                                N_REGIONKEY  INTEGER NOT NULL,
                                N_COMMENT    VARCHAR(152));
            LOAD DATA LOCAL INFILE '${DATA_ROOT}/nation.tbl' INTO TABLE NATION FIELDS TERMINATED BY '|';
        "
    elif [[ $table == "region" ]]
    then
        mysql -p${PASSWORD} --local-infile tpch -e "
            SET GLOBAL local_infile=1;
            CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                                        R_NAME       CHAR(25) NOT NULL,
                                        R_COMMENT    VARCHAR(152));
            LOAD DATA LOCAL INFILE '${DATA_ROOT}/region.tbl' INTO TABLE REGION FIELDS TERMINATED BY '|';
        "
    elif [[ $table == "part" ]]
    then
        mysql -p${PASSWORD} --local-infile tpch -e "
            SET GLOBAL local_infile=1;
            CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                                      P_NAME        VARCHAR(55) NOT NULL,
                                      P_MFGR        CHAR(25) NOT NULL,
                                      P_BRAND       CHAR(10) NOT NULL,
                                      P_TYPE        VARCHAR(25) NOT NULL,
                                      P_SIZE        INTEGER NOT NULL,
                                      P_CONTAINER   CHAR(10) NOT NULL,
                                      P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                                      P_COMMENT     VARCHAR(23) NOT NULL );
            LOAD DATA LOCAL INFILE '${DATA_ROOT}/part.tbl' INTO TABLE PART FIELDS TERMINATED BY '|';
        "
    elif [[ $table == "supplier" ]]
    then
        mysql -p${PASSWORD} --local-infile tpch -e "
            SET GLOBAL local_infile=1;
            CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                                         S_NAME        CHAR(25) NOT NULL,
                                         S_ADDRESS     VARCHAR(40) NOT NULL,
                                         S_NATIONKEY   INTEGER NOT NULL,
                                         S_PHONE       CHAR(15) NOT NULL,
                                         S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                                         S_COMMENT     VARCHAR(101) NOT NULL);
            LOAD DATA LOCAL INFILE '${DATA_ROOT}/supplier.tbl' INTO TABLE SUPPLIER FIELDS TERMINATED BY '|';
        "
    elif [[ $table == "partsupp" ]]
    then
        mysql -p${PASSWORD} --local-infile tpch -e "
            SET GLOBAL local_infile=1;
            CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                                         PS_SUPPKEY     INTEGER NOT NULL,
                                         PS_AVAILQTY    INTEGER NOT NULL,
                                         PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                                         PS_COMMENT     VARCHAR(199) NOT NULL );
            LOAD DATA LOCAL INFILE '${DATA_ROOT}/partsupp.tbl' INTO TABLE PARTSUPP FIELDS TERMINATED BY '|';
        "
    elif [[ $table == "customer" ]]
    then
        mysql -p${PASSWORD} --local-infile tpch -e "
            SET GLOBAL local_infile=1;
            CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                                         C_NAME        VARCHAR(25) NOT NULL,
                                         C_ADDRESS     VARCHAR(40) NOT NULL,
                                         C_NATIONKEY   INTEGER NOT NULL,
                                         C_PHONE       CHAR(15) NOT NULL,
                                         C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                                         C_MKTSEGMENT  CHAR(10) NOT NULL,
                                         C_COMMENT     VARCHAR(117) NOT NULL);
            LOAD DATA LOCAL INFILE '${DATA_ROOT}/customer.tbl' INTO TABLE CUSTOMER FIELDS TERMINATED BY '|';
        "
    elif [[ $table == "orders" ]]
    then
        mysql -p${PASSWORD} --local-infile tpch -e "
            SET GLOBAL local_infile=1;
            CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                                       O_CUSTKEY        INTEGER NOT NULL,
                                       O_ORDERSTATUS    CHAR(1) NOT NULL,
                                       O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                                       O_ORDERDATE      DATE NOT NULL,
                                       O_ORDERPRIORITY  CHAR(15) NOT NULL,  
                                       O_CLERK          CHAR(15) NOT NULL, 
                                       O_SHIPPRIORITY   INTEGER NOT NULL,
                                       O_COMMENT        VARCHAR(79) NOT NULL);
            LOAD DATA LOCAL INFILE '${DATA_ROOT}/orders.tbl' INTO TABLE ORDERS FIELDS TERMINATED BY '|';
        "
    elif [[ $table == "lineitem" ]]
    then
        mysql -p${PASSWORD} --local-infile tpch -e "
            SET GLOBAL local_infile=1;
            CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                                         L_PARTKEY     INTEGER NOT NULL,
                                         L_SUPPKEY     INTEGER NOT NULL,
                                         L_LINENUMBER  INTEGER NOT NULL,
                                         L_QUANTITY    DECIMAL(15,2) NOT NULL,
                                         L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                                         L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                                         L_TAX         DECIMAL(15,2) NOT NULL,
                                         L_RETURNFLAG  CHAR(1) NOT NULL,
                                         L_LINESTATUS  CHAR(1) NOT NULL,
                                         L_SHIPDATE    DATE NOT NULL,
                                         L_COMMITDATE  DATE NOT NULL,
                                         L_RECEIPTDATE DATE NOT NULL,
                                         L_SHIPINSTRUCT CHAR(25) NOT NULL,
                                         L_SHIPMODE     CHAR(10) NOT NULL,
                                         L_COMMENT      VARCHAR(44) NOT NULL);
            LOAD DATA LOCAL INFILE '${DATA_ROOT}/lineitem.tbl' INTO TABLE LINEITEM FIELDS TERMINATED BY '|';
        "
    else
        mysql -p${PASSWORD} --local-infile tpch -e "ERROR: Invalid table name ${table}"
    fi
done
