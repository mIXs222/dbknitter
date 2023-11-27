#!/bin/bash
# File: install_dependencies.sh

# Installing pip for Python package management
sudo apt-get update
sudo apt-get install -y python3-pip

# Installing pymysql and direct_redis
pip3 install pymysql
pip3 install direct_redis

# Installing pandas
pip3 install pandas
