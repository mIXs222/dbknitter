# Bash script to install dependencies (install_dependencies.sh)

#!/bin/bash

# Update package list
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Install pandas
pip3 install pandas

# Install pymysql
pip3 install pymysql

# Install direct_redis
pip3 install direct_redis
