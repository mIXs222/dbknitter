#!/bin/bash

# Update system
sudo apt-get update -y
sudo apt-get upgrade -y

# Install python3 pip
sudo apt-get install python3-pip -y

# Install pymongo using pip
sudo pip3 install pymongo

# Install csv and datetime libraries
sudo pip3 install python-dateutil
