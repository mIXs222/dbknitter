#!/bin/bash
int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 4 ]
then
  echo "Require 4 arguments (DATA_ROOT, MYSQL_TABLES, MONGODB_TABLES, REDIS_TABLES), $# provided"
  echo "Example: tpch_init.sh /path/to/data nation,lineitem,part region,supplier,partsupp,customer orders"
  echo "Example: tpch_init.sh /path/to/data - nation,region,part,supplier,partsupp,customer,orders,lineitem -"
  exit 1
fi

DATA_ROOT=$1
MYSQL_TABLES=$2
MONGODB_TABLES=$3
REDIS_TABLES=$4
echo "Using DATA_ROOT=${DATA_ROOT}, MYSQL_TABLES=${MYSQL_TABLES}, MONGODB_TABLES=${MONGODB_TABLES}, REDIS_TABLES=${REDIS_TABLES}"

if [ ${MYSQL_TABLES} != "-" ]; then
    echo "=========================================================="
    echo "Loading TPC-H to MySQL..."
    docker-compose -f cloudlab/docker-compose.yml exec mysql bash /scripts/tpch_init.sh ${DATA_ROOT} ${MYSQL_TABLES}
    echo ""
fi

if [ ${MONGODB_TABLES} != "-" ]; then
    echo "=========================================================="
    echo "Loading TPC-H to MongoDB..."
    docker-compose -f cloudlab/docker-compose.yml exec mongodb bash /scripts/tpch_init.sh ${DATA_ROOT} ${MONGODB_TABLES}
    echo ""
fi

if [ ${MONGODB_TABLES} != "-" ]; then
    echo "=========================================================="
    echo "Loading TPC-H to Redis..."
    docker-compose -f cloudlab/docker-compose.yml exec redis bash /scripts/tpch_init.sh ${DATA_ROOT} ${REDIS_TABLES}
    echo ""
fi
