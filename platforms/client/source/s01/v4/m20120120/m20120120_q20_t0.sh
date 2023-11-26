#!/bin/bash

# Install Python and pip if they are not already installed
if ! command -v python &>/dev/null;
then
    apt-get update
    apt-get install -y python
fi

if ! command -v pip &>/dev/null;
then
    apt-get install -y python-pip
fi

# Install the required Python libraries
pip install pymysql
pip install pymongo
pip install redis
pip install direct_redis
pip install pandas
