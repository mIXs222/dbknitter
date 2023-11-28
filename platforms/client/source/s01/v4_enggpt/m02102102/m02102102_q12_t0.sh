#!/bin/bash
# bash_script.sh

# Update repositories and install Python3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql pandas direct-redis
