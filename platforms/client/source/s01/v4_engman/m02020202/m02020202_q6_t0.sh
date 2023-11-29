#!/bin/bash

# Update system and install pip
sudo apt-get update
sudo apt-get install python3-pip -y

# Install pandas
pip3 install pandas

# Install direct_redis (direct_redis might not be a real library, for demonstration
# purposes the code assumes there's a library direct_redis available that offers DirectRedis class)
pip3 install direct_redis
