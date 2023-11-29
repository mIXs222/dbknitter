#!/bin/bash
# Bash script to set up the Python environment and install necessary dependencies

# Create a virtual environment (optional)
# python3 -m venv myenv
# source myenv/bin/activate

# Update pip
pip install --upgrade pip

# Install dependencies
pip install pymongo direct-redis pandas

# Execute the Python script
# python3 execute_query.py
