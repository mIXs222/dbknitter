#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3, pip and MySQL dependencies
sudo apt-get install -y python3 python3-pip python3-dev default-libmysqlclient-dev build-essential

# Install Python dependencies
pip3 install pymongo pymysql

# Note: The above script assumes that the user is on a Debian/Ubuntu-based system.
# If the system is different (e.g., RedHat-based), package installation commands will vary.
