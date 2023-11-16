#!/bin/bash
set -e

# Ensure Python3 and Pip are installed
if ! command -v python3 &> /dev/null; then
    echo "Python3 is not installed. Installing Python3..."
    sudo apt-get update -y
    sudo apt-get install python3.6 -y
fi

if ! command -v pip &> /dev/null; then
    echo "Pip is not installed. Installing Pip..."
    sudo apt-get install python3-pip -y
fi

# Install pymongo library for MongoDB connection
echo "Installing pymongo..."
pip install pymongo
