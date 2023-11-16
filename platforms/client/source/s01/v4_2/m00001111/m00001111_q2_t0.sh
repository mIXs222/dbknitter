#!/bin/bash

# update package lists
apt-get update

# install python3 and pip
apt-get install -y python3
apt-get install -y python3-pip

# install python libraries
pip3 install pymysql pymongo pandas
