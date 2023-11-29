#!/bin/bash
# install_dependencies.sh

# Updating package lists
sudo apt-get update -y

# Installing pip
sudo apt-get install python3-pip -y

# Installing required Python libraries
pip3 install pymysql pandas redis direct_redis
