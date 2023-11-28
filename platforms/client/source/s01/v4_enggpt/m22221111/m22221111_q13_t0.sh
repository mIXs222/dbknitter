#!/bin/bash

# Update the repositories and upgrade the system
sudo apt-get update && sudo apt-get upgrade -y

# Install pip and Python MongoDB driver (pymongo)
sudo apt-get install -y python3-pip
pip3 install pymongo
