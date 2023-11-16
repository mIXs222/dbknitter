#!/bin/bash

# Update package list and upgrade existing packages
apt-get update && apt-get upgrade -y

# Install pip and Python development packages if they're not installed
apt-get install -y python3-pip python3-dev

# Install required pip packages
pip3 install pymysql pandas redis direct-redis
