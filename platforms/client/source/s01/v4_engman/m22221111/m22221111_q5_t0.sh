#!/bin/bash
# install_dependencies.sh

# update packages and install system dependencies
sudo apt update
sudo apt install -y python3 python3-pip mongodb redis-server

# install python dependencies
pip3 install pymongo redis pandas direct-redis
