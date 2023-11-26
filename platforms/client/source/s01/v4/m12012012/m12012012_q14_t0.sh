#!/bin/bash

# Ensure script is run as root
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

# Install Python and pip
apt-get update && apt-get install -y python3 python3-pip

# Install pymysql and pandas Python libraries
pip3 install pymysql pandas

# Install the direct_redis Python library 
pip3 install git+https://github.com/pfreixes/direct_redis
