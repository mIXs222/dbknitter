#!/bin/bash

# Update repositories and install Python3 and pip if they're not already installed
sudo apt update 
sudo apt install -y python3 python3-pip

# Install pymongo to interact with MongoDB from Python
pip3 install pymongo
