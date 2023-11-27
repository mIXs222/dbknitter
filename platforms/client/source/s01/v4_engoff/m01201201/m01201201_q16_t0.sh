#!/bin/bash

# Install Python and pip
apt-get update && apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct_redis
