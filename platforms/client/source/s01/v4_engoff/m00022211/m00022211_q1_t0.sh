#!/bin/bash

set -e

# Update the package list
apt-get update

# Install Python and Pip if they are not installed
apt-get install -y python3 python3-pip

# Install pymongo library
pip3 install pymongo
