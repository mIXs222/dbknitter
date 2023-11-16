#!/bin/bash
# setup.sh

# Update and install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas direct_redis
