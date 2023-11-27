#!/bin/bash
# setup.sh

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Install necessary Python packages
pip install pandas
pip install direct-redis

# Run the Python script
python main.py

# Deactivate the virtual environment
deactivate
