#!/bin/bash

# Install Python and Pip
apt-get update
apt-get install python3 python3-pip -y

# Install pymongo and pandas
pip3 install pymongo pandas

# Install direct_redis, which may involve getting the package from a repository or from the project's website
pip3 install direct_redis
