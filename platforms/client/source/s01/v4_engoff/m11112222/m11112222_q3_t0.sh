#!/bin/bash

# bash script to install dependencies to run the python code

# Update repositories and upgrade packages
sudo apt-get update
sudo apt-get upgrade -y

# Install pip and Python dev packages
sudo apt-get install -y python3-pip python3-dev

# Install direct_redis and pandas using pip
pip3 install direct-redis pandas
