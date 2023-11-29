#!/bin/bash

# Update system and install python3 and pip if not installed
sudo apt-get update -y
sudo apt-get install python3 -y
sudo apt-get install python3-pip -y

# Install pymongo to interact with MongoDB
pip3 install pymongo
