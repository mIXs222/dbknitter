#!/bin/bash

# Install Python and pip if they are not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the necessary Python packages
pip3 install pymongo pandas redis direct-redis
