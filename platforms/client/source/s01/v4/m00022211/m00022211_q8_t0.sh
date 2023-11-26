# install_dependencies.sh

#!/bin/bash

# Update package list and install Python pip
apt-get update
apt-get install -y python3-pip

# Install pymysql, pymongo, pandas, and direct_redis
pip3 install pymysql pymongo pandas direct_redis
