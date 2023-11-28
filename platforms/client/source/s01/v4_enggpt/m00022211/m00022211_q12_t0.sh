#!/bin/bash
set -e

# Update package list, upgrade packages and install pip
apt-get update
apt-get -y upgrade
apt-get install -y python3-pip

# Install required Python package
pip3 install pymongo
