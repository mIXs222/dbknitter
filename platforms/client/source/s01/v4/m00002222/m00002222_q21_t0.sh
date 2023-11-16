#!/bin/bash

# Update package list
sudo apt update

# Install python3 and pip
sudo apt install -y python3 python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas

# Install direct_redis (this assumes it is available for installation)
pip3 install direct_redis
