# setup.sh
#!/bin/bash

# Update package repository
apt-get update -y

# Required package installation for MySQL client library
apt-get install -y default-libmysqlclient-dev

# Python3 pip installation if it's not already installed.
apt-get install -y python3-pip

# Install pymysql Python library
pip3 install pymysql
