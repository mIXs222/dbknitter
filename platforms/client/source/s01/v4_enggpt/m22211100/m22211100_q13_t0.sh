#!/bin/bash

# Updating package list
apt-get update

# Installing Python and PIP
apt-get install -y python3 python3-pip

# Installing Python dependencies
pip3 install pymysql pymongo
