#!/bin/bash

# update repositories and install pip
apt-get update
apt-get install -y python3-pip

# install Python packages
pip3 install pandas
pip3 install pymongo
pip3 install direct_redis
