#!/bin/bash

# Update package list and upgrade existing packages
sudo apt-get update && sudo apt-get -y upgrade

# Install pip3 and required system packages
sudo apt-get install -y python3-pip

# Install Python packages
pip3 install pymysql pymongo pandas

# Clone the "direct_redis" Git repository and install it
git clone https://github.com/dongfuhai/direct_redis.git
cd direct_redis
pip3 install .

# Go back to the previous directory
cd ..
