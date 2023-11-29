#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip if it's not installed
sudo apt-get install -y python3-pip

# Install dependencies using pip
pip3 install pandas direct_redis
