#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt-get update

# Install MySQL client and pip
sudo apt-get install -y default-libmysqlclient-dev python3-pip 

# Install the Python MongoDB driver
sudo pip3 install pymongo pymysql direct_redis pandas
