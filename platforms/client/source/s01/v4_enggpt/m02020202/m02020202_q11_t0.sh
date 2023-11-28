#!/bin/bash

# Update and Upgrade existing packages
apt-get update
apt-get upgrade -y

# Install Python and pip
apt-get install python3 -y
apt-get install python3-pip -y

# Install Python library dependencies
pip3 install pymysql
pip3 install direct_redis
pip3 install pandas
