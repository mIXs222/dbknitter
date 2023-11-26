#!/bin/bash

# Update and Upgrade the System
sudo apt-get update
sudo apt-get upgrade -y

# Install pip and Python required dev tools
sudo apt-get install python3-pip python3-dev -y

# Install Python libraries
pip3 install pymongo pandas redis direct-redis
