#!/bin/bash

# Update package lists
apt-get update

# Install pip and Python development package if they are not installed
apt-get install -y python3-pip python3-dev

# Install necessary Python libraries
pip3 install pymysql direct_redis pandas
