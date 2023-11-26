#!/bin/bash

# Updating the package list and upgrading the existing packages
sudo apt-get update
sudo apt-get -y upgrade

# Installing Python and pip
sudo apt-get install -y python3 python3-pip

# Installing the required Python libraries
pip3 install pymysql pandas redis direct_redis
