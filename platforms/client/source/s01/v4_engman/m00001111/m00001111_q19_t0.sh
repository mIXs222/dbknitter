#!/bin/bash

# Update package list and install Python pip if not already installed
apt-get update
apt-get install -y python3-pip

# Install the necessary Python libraries for the project
pip3 install pymysql pymongo
