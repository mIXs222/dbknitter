#!/bin/bash

# Update repository and upgrade packages
sudo apt-get update -y
sudo apt-get upgrade -y

# Install Python3 and pip
sudo apt-get install python3 -y
sudo apt-get install python3-pip -y

# Install required Python libraries
pip3 install pymysql pymongo pandas
pip install git+https://github.com/redis/direct_redis.git
