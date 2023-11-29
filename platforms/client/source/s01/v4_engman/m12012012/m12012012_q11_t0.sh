#!/bin/bash

# Update packages and install pip
apt-get update
apt-get install -y python3-pip

# Install pymongo, redis and pandas
pip3 install pymongo redis pandas direct-redis
