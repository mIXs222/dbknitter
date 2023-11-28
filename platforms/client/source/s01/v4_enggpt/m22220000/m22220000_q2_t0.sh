# install_dependencies.sh

#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and pip if not already installed
sudo apt-get install -y python3-pip

# Install the pymysql library
pip3 install pymysql

# Install pandas library
pip3 install pandas

# Install the redis-py library via DirectRedis fork
pip3 install git+https://github.com/amyangfei/direct_redis.git
