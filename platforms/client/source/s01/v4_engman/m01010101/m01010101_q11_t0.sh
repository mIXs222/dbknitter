#!/bin/bash

# Updating package list
sudo apt-get update

# Install pip if not already installed
sudo apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql

# Install pymongo
pip3 install pymongo
