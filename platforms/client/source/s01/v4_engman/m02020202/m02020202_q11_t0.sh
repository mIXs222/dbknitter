#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python and pip
sudo apt-get install python3 python3-pip -y

# Install pymysql and pandas
pip3 install pymysql pandas

# Install redis-py and direct_redis
pip3 install redis direct_redis
