#!/bin/bash

# Update package lists
apt-get update

# Install pip if it is not already installed
apt-get -y install python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas

# Install custom direct_redis library (assuming it is available via pip or add relevant installation commands if it's not)
pip3 install direct_redis
