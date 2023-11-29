#!/bin/bash

# Update repositories and install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# MongoDB driver
pip3 install pymongo

# Redis driver and pandas
pip3 install direct-redis pandas
