#!/bin/bash
# install python3 and pip3
apt-get update
apt-get install -y python3 python3-pip

# install required python packages
pip3 install pandas pymysql direct-redis
