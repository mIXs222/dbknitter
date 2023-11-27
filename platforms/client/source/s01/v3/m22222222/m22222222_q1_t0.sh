#!/bin/bash

# Install Python and pip
sudo apt update
sudo apt install -y python3.7
sudo apt install -y python3-pip

# Install required Python libraries
pip3 install pandas
pip3 install direct_redis
