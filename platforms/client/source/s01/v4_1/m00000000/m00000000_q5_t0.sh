#!/bin/bash
# install pip if not already installed
if ! command -v pip &> /dev/null
then
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    python get-pip.py
fi

# install python dependencies
pip install pandas
pip install pymysql
pip install pymongo
pip install redis
pip install direct_redis
pip install csv
