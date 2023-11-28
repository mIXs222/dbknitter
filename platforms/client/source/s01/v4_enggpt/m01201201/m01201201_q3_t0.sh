#!/bin/bash

# install_dependencies.sh

# Update repositories and install pip for Python 3
sudo apt-get update -y
sudo apt-get install python3-pip -y

# Install the Python libraries for MySQL, MongoDB, and Redis data access
pip3 install pymysql pymongo pandas direct_redis
