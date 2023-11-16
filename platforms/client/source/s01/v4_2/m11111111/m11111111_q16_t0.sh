#!/bin/bash

# Update packages
apt-get update -y

# Install python 
apt-get install python3.6 -y

# Install pip
apt-get install python3-pip -y

# Install MongoDB server
apt-get install -y mongodb-org

# Install required python libraries
pip3 install pymongo pandas
