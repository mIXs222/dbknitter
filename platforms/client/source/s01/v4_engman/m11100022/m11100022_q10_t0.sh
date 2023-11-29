#!/bin/bash

# install_dependencies.sh
sudo apt-get update

# Install Python and Pip
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo pandas redis_direct

# Note: DirectRedis may have specific installation requirements which are not covered here.
# You should install any additional dependencies it may have.
