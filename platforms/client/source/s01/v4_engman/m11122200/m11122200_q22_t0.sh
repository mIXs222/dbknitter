#!/bin/bash

# Update package list and install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql, direct_redis, and pandas
pip3 install pymysql
pip3 install direct_redis
pip3 install pandas
