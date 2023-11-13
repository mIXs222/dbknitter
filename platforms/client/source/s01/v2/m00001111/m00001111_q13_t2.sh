#!/bin/bash

# Update package lists
apt-get update -y

# Install Python and Pip
apt-get install -y python3 python3-pip

# Install python dependencies
pip3 install pymongo pandas pandasql
