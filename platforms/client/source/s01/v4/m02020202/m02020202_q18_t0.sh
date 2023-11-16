#!/bin/bash
# install_dependencies.sh

# Update and install python3 and pip
apt-get update && apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pandas pymysql direct-redis
