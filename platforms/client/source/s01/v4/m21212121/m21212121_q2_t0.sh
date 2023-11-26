#!/bin/bash

# Update and install required packages
sudo apt-get update && sudo apt-get install -y python3 python3-pip

# Install required python libraries
pip3 install pymongo pandas redis direct_redis
