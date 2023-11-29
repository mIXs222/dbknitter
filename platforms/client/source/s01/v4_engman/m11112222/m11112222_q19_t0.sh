#!/bin/bash

# Update packages list and Upgrade system
apt-get update -y && apt-get upgrade -y

# MongoDB dependencies
apt-get install -y libssl-dev
pip install pymongo

# Redis dependencies
pip install redis
pip install direct_redis
pip install pandas
