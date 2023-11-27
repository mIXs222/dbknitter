#!/bin/bash
# Bash script (install_dependencies.sh)

# Update packages and install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas

# Install direct_redis from source as it is not available on PyPI
git clone https://github.com/coleifer/direct-redis.git
cd direct-redis
python3 setup.py install
cd ..
rm -rf direct-redis
