#!/bin/bash

# Initialize the shell environment for python3
python3 -m venv venv
source venv/bin/activate

# Install pymongo using pip (for MongoDB connection)
pip install pymongo

# Install pandas for handling data
pip install pandas
