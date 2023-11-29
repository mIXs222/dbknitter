#!/bin/bash

# Update package list and install Python and pip
apt-get update
apt-get install -y python3 python3-pip

# Install necessary Python packages
pip3 install pymysql direct_redis pandas
