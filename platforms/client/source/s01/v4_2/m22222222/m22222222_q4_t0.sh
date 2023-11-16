#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and Pip
sudo apt-get install python3.7
sudo apt-get install python3-pip

# Install Redis
sudo apt-get install redis-server

# Install Python dependencies
pip3 install redis pandas
