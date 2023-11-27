#!/bin/bash

# Update package list and upgrade packages
apt-get update
apt-get upgrade -y

# Install Python and Pip if they are not installed
apt-get install -y python3 python3-pip

# Install Python libraries for MySQL, MongoDB, Redis, and Pandas
pip3 install pymysql pymongo redis pandas direct_redis
