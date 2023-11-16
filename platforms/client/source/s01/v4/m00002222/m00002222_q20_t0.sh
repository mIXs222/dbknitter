#!/bin/bash
# install_dependencies.sh

# Update and install system packages
sudo apt update && sudo apt install -y python3-pip

# Install the necessary Python libraries
pip3 install pymysql pandas direct-redis
