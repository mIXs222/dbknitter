#!/bin/bash
set -e

# Ensure Python 3 and pip are installed
command -v python3 &>/dev/null || { echo "Python 3 is not installed."; exit 1; }
command -v pip3 &>/dev/null || { echo "pip for Python 3 is not installed."; exit 1; }

# Install pymongo to interact with the MongoDB database
pip3 install pymongo
