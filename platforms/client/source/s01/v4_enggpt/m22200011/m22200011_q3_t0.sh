#!/bin/bash

# Update the package list
apt-get update -y

# Install Python and pip
apt-get install python3 python3-pip -y

# Install Python libraries required for the script
pip3 install PyMySQL pymongo pandas
