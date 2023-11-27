#!/bin/bash

# Update and install pip if it's not already installed
apt-get update
apt-get install -y python3-pip

# Install pymysql and pymongo Python packages
pip3 install pymysql pymongo
